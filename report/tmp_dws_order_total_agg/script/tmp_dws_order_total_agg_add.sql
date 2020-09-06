set hive.execution.engine=spark;
drop table if exists dws.tmp_dws_order_total_agg_add;
create table if not exists dws.tmp_dws_order_total_agg_add (
	report_date timestamp comment "统计时间"
	,product_name string comment "产品名称"
	,sales_user_name string comment "渠道经理"
	,sales_user_id string comment "渠道经理id"
	,sales_branch_id string comment "所属部门id"
	,branch_id string comment "分公司id"
	,interview_noloan_num double comment "面签未放款订单"
	,loan_not_repaid_num double comment "回款在途订单数"
	,not_financial_archive_num double comment "未财务归档订单"
	,etl_update_time timestamp
);

with tmp_report_date as (
	select 
	calendar report_date 
	from dws.dimension_date bao
	where calendar = '${hivevar:logdate}'
	group by calendar
)

insert overwrite table dws.tmp_dws_order_total_agg_add
select 
union_end.report_date
,union_end.product_name 
,union_end.sales_user_name
,union_end.sales_user_id
,union_end.sales_branch_id
,union_end.branch_id
,sum(union_end.interview_noloan_num) interview_noloan_num -- 面签未放款订单
,sum(union_end.loan_not_repaid_num) loan_not_repaid_num -- 回款在途订单数
,sum(union_end.not_financial_archive_num) not_financial_archive_num -- 未财务归档订单
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from (
	select 
	trd.report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,count(distinct bao.apply_no) interview_noloan_num -- 面签未放款订单
	,0 loan_not_repaid_num -- 回款在途订单数
	,0 not_financial_archive_num -- 未财务归档订单
	from ods.ods_bpms_biz_apply_order_common bao, tmp_report_date trd
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
	left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
	where to_date(bcocew.interview_time_min) <= trd.report_date and (fac.loan_time_xg is null or to_date(fac.loan_time_xg) > trd.report_date) 
	group by trd.report_date, bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id, bao.branch_id

	union all

	select 
	trd.report_date
	,bao.product_name 
	,bao.sales_user_name
	,bao.sales_user_id
	,bao.sales_branch_id
	,bao.branch_id
	,0 interview_noloan_num -- 面签未放款订单
	,count(distinct bao.apply_no) loan_not_repaid_num -- 回款在途订单数
	,0 not_financial_archive_num -- 未财务归档订单
	from ods.ods_bpms_biz_apply_order_common bao, tmp_report_date trd
	left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
	left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
	where fac.return_status in ("未回款", "回款中") 
	and (bcocew.financialarchiveevent_time is null or bcocew.financialarchiveevent_time > trd.report_date)
	group by trd.report_date, bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id, bao.branch_id

	union all

	select 
	a.report_date
	,a.product_name 
	,a.sales_user_name
	,a.sales_user_id
	,a.sales_branch_id
	,a.branch_id
	,0 interview_noloan_num -- 面签未放款订单
	,0 loan_not_repaid_num -- 回款在途订单数
	,sum(a.not_financial_archive_num) not_financial_archive_num --  未财务归档订单 
	from (
		select 
		trd.report_date
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,count(distinct bao.apply_no) not_financial_archive_num --  未财务归档订单
		from ods.ods_bpms_biz_apply_order_common bao, tmp_report_date trd
		left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
		left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
		where fac.return_status = "回款完结" and bao.product_type = "现金类产品"
		and (bcocew.financialarchiveevent_time is null or to_date(bcocew.financialarchiveevent_time) > trd.report_date)
		group by trd.report_date, bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id, bao.branch_id

		union all

		select 
		trd.report_date
		,bao.product_name 
		,bao.sales_user_name
		,bao.sales_user_id
		,bao.sales_branch_id
		,bao.branch_id
		,count(distinct bao.apply_no) not_financial_archive_num --  未财务归档订单
		from ods.ods_bpms_biz_apply_order_common bao, tmp_report_date trd
		left join ods.ods_bpms_bpm_check_opinion_common_ex_wx bcocew on bao.apply_no = bcocew.apply_no
		left join ods.ods_finance_agg_common fac on bao.apply_no = fac.apply_no
		where bao.product_type = "保险类产品" and fac.loan_time_xg <= trd.report_date
		and (bcocew.financialarchiveevent_time is null or to_date(bcocew.financialarchiveevent_time) > trd.report_date)
		group by trd.report_date, bao.product_name, bao.sales_user_name, bao.sales_user_id, bao.sales_branch_id, bao.branch_id
	) a
	group by a.report_date, a.product_name, a.sales_user_name, a.sales_user_id, a.sales_branch_id, a.branch_id
) union_end 
group by union_end.report_date, union_end.product_name, union_end.sales_user_name, union_end.sales_user_id, union_end.sales_branch_id, union_end.branch_id;
;
