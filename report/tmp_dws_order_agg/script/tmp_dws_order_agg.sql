set hive.execution.engine=spark;
drop table if exists dws.tmp_dws_order_agg;
CREATE TABLE dws.tmp_dws_order_agg (
	report_date timestamp comment "统计时间"
	,product_name string comment "产品名称"
	,sales_user_name string comment "渠道经理"
	,sales_user_id string comment "渠道经理id"
	,sales_branch_id string comment "所属部门id"
	,branch_id string comment "分公司"
	,partner_insurance_name string comment "合作机构"
	,partner_bank_name string comment "合作银行"
	,new_order_num double comment "新增订单量"
	,interview_order_num double comment "面签订单量"
	,interview_xg_num double comment "面签笔数"
	,agreeloanmark_order_num double comment "同贷订单量"
	,loan_order_num double comment "放款订单量"
	,loan_xg_num double comment "放款笔数"
	,release_amount_xg double comment "放款金额"
	,manual_end_order_num double comment "退单量"
	,input_info_order_num double comment "录入发生订单量"
	,input_info_end_order_num double comment "录入提交订单量"
	,input_materials_order_num double comment "录入补件数量"
	,send_material_order_num double comment "外传处理订单量"
	,input_info_complete_order_num double comment "录入完成处理订单量"
	,etl_update_time timestamp 
);
with tmp_report_date as (
	select 
	to_date(bao.apply_time) report_date 
	from ods.ods_bpms_biz_apply_order_common bao
-- 	where apply_time >= '${hivevar:report_date_start}' and apply_time < '${hivevar:report_date_end}'
	group by to_date(bao.apply_time)
),

tmp_matter_record_ct AS (
	select
	a.apply_no
	,min(CASE WHEN LOWER(matter_key)='inputinfo' THEN CAST(create_time AS STRING) END) inputinfo_time
	,min(CASE WHEN LOWER(matter_key)='uploadimg' THEN CAST(create_time AS STRING) END) uploadimg_time
	from (
		SELECT *
		,row_number() over(PARTITION BY apply_no,matter_key ORDER BY create_time ASC) rk
		FROM ods.ods_bpms_biz_order_matter_record_common
		where create_time is not null and LOWER(matter_key) in ('inputinfo', 'uploadimg')
	) as a
	where a.rk = 1
	group by a.apply_no
),

tmp_matter_record_ht AS (
	select
	a.apply_no
	,min(CASE WHEN LOWER(matter_key)='inputinfo' THEN CAST(handle_time AS STRING) END) inputinfo_time
	,min(CASE WHEN LOWER(matter_key)='applycheck' THEN CAST(handle_time AS STRING) END) applycheck_time
	from (
		SELECT *
		,row_number() over(PARTITION BY apply_no, matter_key ORDER BY handle_time) rk
		FROM ods.ods_bpms_biz_order_matter_record_common
		where handle_time is not null and LOWER(matter_key) in ('inputinfo', 'applycheck')
	) as a
	where a.rk = 1
	group by a.apply_no
),

tmp_matter_record_ht_max AS (
	select
	a.apply_no
	,min(CASE WHEN LOWER(matter_key)='checkmarsend' THEN CAST(handle_time AS STRING) END) checkmarsend_time
	,min(CASE WHEN LOWER(matter_key)='sendverifymaterial' THEN CAST(handle_time AS STRING) END) sendverifymaterial_time
	from (
		SELECT *
		,row_number() over(PARTITION BY apply_no, matter_key ORDER BY handle_time desc) rk
		FROM ods.ods_bpms_biz_order_matter_record_common
		where handle_time is not null and LOWER(matter_key) in ('checkmarsend', 'sendverifymaterial')
	) as a
	where a.rk = 1
	group by a.apply_no
),

tmp_check_opinion_cpt AS (
	SELECT
	PROC_INST_ID_
	,min(CASE WHEN LOWER(TASK_KEY_)='usertask2' THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask2_time
	from (
		SELECT 
		*
		,row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY COMPLETE_TIME_ ) rn
		FROM ods.ods_bpms_bpm_check_opinion
		where COMPLETE_TIME_ is not null and LOWER(TASK_KEY_) in ("usertask2")
	) as  
	where rn = 1
	group by PROC_INST_ID_
),

tmp_check_opinion_cpt_max AS (
	SELECT
	PROC_INST_ID_
	,min(CASE WHEN LOWER(TASK_KEY_)='usertask7' THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask7_time
	from ods.ods_bpms_bpm_check_opinion_common_ex
	where rn = 1 and COMPLETE_TIME_ is not null and LOWER(TASK_KEY_) in ("usertask7")
	group by PROC_INST_ID_
),

tmp_tjlr_date as (
	select * from 
	(
		select 
		bao.apply_no
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,bao.partner_insurance_name
		,bao.partner_bank_name
		,IF( bim.apply_order_attach_complete_time > CAST(tmr_ct.inputinfo_time AS TIMESTAMP)
			, bim.apply_order_attach_complete_time
			, CAST(tmr_ct.inputinfo_time AS TIMESTAMP)
	    ) tjlr_date -- '提交录入时间（变红时间）'
	    from ods.ods_bpms_biz_apply_order_common bao
	    LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim ON bao.apply_no = bim.apply_no
	    left join tmp_matter_record_ct tmr_ct on bao.apply_no = tmr_ct.apply_no
    )as a 
    where a.tjlr_date is not null 
),

tmp_lrtj_date as (
	select * from 
	(
		select 
		bao.apply_no
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,bao.partner_insurance_name
		,bao.partner_bank_name
		,CASE WHEN bao.product_version NOT IN ('v1', 'v1.5') THEN (tmr_ht.inputinfo_time)
	     	  WHEN bao.product_version IN ('v1', 'v1.5') THEN (tco_cpt.usertask2_time)
	     END lrtj_date -- 录入提交时间
	    from ods.ods_bpms_biz_apply_order_common bao
	    LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim ON bao.apply_no = bim.apply_no
	    left join tmp_matter_record_ht tmr_ht on bao.apply_no = tmr_ht.apply_no
	    left join tmp_check_opinion_cpt tco_cpt on bao.flow_instance_id = tco_cpt.PROC_INST_ID_
    )as a 
    where a.lrtj_date is not null 

),

tmp_ccth_date as (
	select * from 
	(
		select 
		bao.apply_no
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,bao.partner_insurance_name
		,bao.partner_bank_name
		,CASE WHEN (tmr_ht.applycheck_time) IS NULL OR (tmr_ht.applycheck_time)=''
                    THEN (tmr_ct.uploadimg_time)
		      ELSE CAST(if(tmr_ct.uploadimg_time < tmr_ht.applycheck_time, tmr_ct.uploadimg_time, null) AS STRING)
		 END ccth_date -- 初次退回时间
	    from ods.ods_bpms_biz_apply_order_common bao
	    LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim ON bao.apply_no = bim.apply_no
	    left join tmp_matter_record_ht tmr_ht on bao.apply_no = tmr_ht.apply_no
	    left join tmp_matter_record_ct tmr_ct on bao.apply_no = tmr_ct.apply_no
    )as a 
    where a.ccth_date is not null 
),

tmp_spzlwc_date as (
	select * from 
	(
		select 
		bao.apply_no
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,bao.partner_insurance_name
		,bao.partner_bank_name
		,CASE WHEN bao.product_version IN ('v2.5', 'v2.0') AND bao.product_type = "现金类产品"
	    	THEN (tmr_ht_max.checkmarsend_time)

	    	WHEN bao.product_version IN ('v2.5', 'v2.0') AND bao.product_type = "保险类产品"
	    	THEN (tmr_ht_max.sendverifymaterial_time)

	    	WHEN bao.product_version IN ('v1.5', 'v1.0')
	    	THEN (tco_cpt_max.usertask7_time)

		 END spzlwc_date -- 审批资料外传/核保_处理时间
	    from ods.ods_bpms_biz_apply_order_common bao
	    LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim ON bao.apply_no = bim.apply_no
	    left join tmp_matter_record_ht_max tmr_ht_max on bao.apply_no = tmr_ht_max.apply_no
	    left join tmp_check_opinion_cpt_max tco_cpt_max on bao.flow_instance_id = tco_cpt_max.PROC_INST_ID_
    )as a 
    where a.spzlwc_date is not null 
)

insert overwrite table dws.tmp_dws_order_agg
select 
union_end.report_date
,union_end.product_name 
,union_end.sales_user_name
,union_end.sales_user_id
,union_end.sales_branch_id
,union_end.branch_id
,union_end.partner_insurance_name
,union_end.partner_bank_name
,sum(new_order_num) new_order_num -- 新增订单量
,sum(interview_order_num) interview_order_num -- 面签订单量
,sum(interview_xg_num) interview_xg_num -- 面签笔数
,sum(agreeloanmark_order_num) agreeloanmark_order_num -- 同贷订单量
,sum(loan_order_num) loan_order_num -- 放款订单量
,sum(loan_xg_num) loan_xg_num -- 放款笔数
,sum(release_amount_xg) release_amount_xg -- 放款金额
,sum(manual_end_order_num) manual_end_order_num -- 退单量
,sum(input_info_order_num) input_info_order_num -- 录入发生订单量
,sum(input_info_end_order_num) input_info_end_order_num -- 录入提交订单量
,sum(input_materials_order_num) input_materials_order_num -- 录入补件数量
,sum(send_material_order_num) send_material_order_num -- 外传处理订单量
,sum(input_info_complete_order_num) input_info_complete_order_num-- 录入完成处理订单量
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from( 
	select 
	to_date(bao.apply_time) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,count(distinct bao.apply_no) new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao 
	join tmp_report_date trd on to_date(bao.apply_time) = trd.report_date
	group by to_date(bao.apply_time), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(bcocew.interview_time_min) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,count(distinct bao.apply_no) interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
	join tmp_report_date trd on to_date(bcocew.interview_time_min) = trd.report_date
	group by to_date(bcocew.interview_time_min), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(bcocew.interview_time_min) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,sum(ooac.interview_num_xg) interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
	join tmp_report_date trd on to_date(bcocew.interview_time_min) = trd.report_date
	left join ods.ods_order_agg_common ooac on bao.apply_no = ooac.apply_no
	group by to_date(bcocew.interview_time_min), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(bcocew.agreeloanmark_time) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,count(distinct bao.apply_no) agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
	join tmp_report_date trd on to_date(bcocew.agreeloanmark_time) = trd.report_date
	group by to_date(bcocew.agreeloanmark_time), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(fac.loan_time_xg) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,count(distinct bao.apply_no) loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
	join tmp_report_date trd on to_date(fac.loan_time_xg) = trd.report_date
	group by to_date(fac.loan_time_xg), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(fac.loan_time_xg) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,sum(fac.loan_num_xg) loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
	join tmp_report_date trd on to_date(fac.loan_time_xg) = trd.report_date
	group by to_date(fac.loan_time_xg), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(fac.loan_time_xg) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,sum(fac.release_amount_xg) release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
	join tmp_report_date trd on to_date(fac.loan_time_xg) = trd.report_date
	group by to_date(fac.loan_time_xg), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all

	select 
	to_date(oac.manual_end_time) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,count(distinct bao.apply_no) manual_end_order_num -- 退单量
	,0 input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_order_agg_common oac on bao.apply_no = oac.apply_no
	join tmp_report_date trd on to_date(oac.manual_end_time) = trd.report_date
	group by to_date(oac.manual_end_time), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

	union all 

	-- ----------------------------- 新增
	select 
	to_date(ttd.tjlr_date) report_date
	,ttd.product_name 
	,ttd.sales_user_name
	,ttd.sales_user_id
	,ttd.sales_branch_id
	,ttd.branch_id
	,ttd.partner_insurance_name
	,ttd.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,count(distinct ttd.apply_no) input_info_order_num-- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from tmp_tjlr_date ttd
	group by to_date(ttd.tjlr_date), ttd.product_name, ttd.sales_user_name, ttd.sales_user_id, ttd.sales_branch_id
	, ttd.branch_id, ttd.partner_insurance_name, ttd.partner_bank_name

	union all

	select 
	to_date(tld.lrtj_date) report_date
	,tld.product_name 
	,tld.sales_user_name
	,tld.sales_user_id
	,tld.sales_branch_id
	,tld.branch_id
	,tld.partner_insurance_name
	,tld.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num -- 录入发生订单量
	,count(tld.apply_no) input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from tmp_lrtj_date tld
	group by to_date(tld.lrtj_date), tld.product_name, tld.sales_user_name, tld.sales_user_id, tld.sales_branch_id
	, tld.branch_id, tld.partner_insurance_name, tld.partner_bank_name

	union all

	select 
	to_date(tcd.ccth_date) report_date
	,tcd.product_name 
	,tcd.sales_user_name
	,tcd.sales_user_id
	,tcd.sales_branch_id
	,tcd.branch_id
	,tcd.partner_insurance_name
	,tcd.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num -- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,count(tcd.apply_no) input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from tmp_ccth_date tcd
	group by to_date(tcd.ccth_date), tcd.product_name, tcd.sales_user_name, tcd.sales_user_id, tcd.sales_branch_id
	, tcd.branch_id, tcd.partner_insurance_name, tcd.partner_bank_name

	union all

	select 
	to_date(tsd.spzlwc_date) report_date
	,tsd.product_name 
	,tsd.sales_user_name
	,tsd.sales_user_id
	,tsd.sales_branch_id
	,tsd.branch_id
	,tsd.partner_insurance_name
	,tsd.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num -- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,count(tsd.apply_no) send_material_order_num -- 外传处理订单量
	,0 input_info_complete_order_num -- 录入完成处理订单量
	from tmp_spzlwc_date tsd
	group by to_date(tsd.spzlwc_date), tsd.product_name, tsd.sales_user_name, tsd.sales_user_id, tsd.sales_branch_id
	, tsd.branch_id, tsd.partner_insurance_name, tsd.partner_bank_name

	union all

	select 
	to_date(bcoo.inputinfocomplete_time) report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,bao.partner_insurance_name
	,bao.partner_bank_name
	,0 new_order_num -- 新增订单量
	,0 interview_order_num -- 面签订单量
	,0 interview_xg_num -- 面签笔数
	,0 agreeloanmark_order_num -- 同贷订单量
	,0 loan_order_num -- 放款订单量
	,0 loan_xg_num -- 放款笔数
	,0 release_amount_xg -- 放款金额
	,0 manual_end_order_num -- 退单量
	,0 input_info_order_num -- 录入发生订单量
	,0 input_info_end_order_num -- 录入提交订单量
	,0 input_materials_order_num -- 录入补件数量
	,0 send_material_order_num -- 外传处理订单量
	,count(bcoo.apply_no) input_info_complete_order_num -- 录入完成处理订单量
	from ods.ods_bpms_biz_apply_order_common bao
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcoo on bao.apply_no = bcoo.apply_no
	join tmp_report_date trd on to_date(bcoo.inputinfocomplete_time) = trd.report_date
	group by to_date(bcoo.inputinfocomplete_time), bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id
	, bao.branch_id, bao.partner_insurance_name, bao.partner_bank_name

) as union_end
group by union_end.report_date, union_end.product_name, union_end.sales_user_name, union_end.sales_user_id
, union_end.sales_branch_id, union_end.branch_id, union_end.sales_branch_id, union_end.branch_id, union_end.partner_insurance_name, union_end.partner_bank_name;