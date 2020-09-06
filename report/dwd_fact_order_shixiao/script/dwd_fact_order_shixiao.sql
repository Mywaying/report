SET hive.execution.engine=spark;
USE ods ;
DROP TABLE IF EXISTS dwd.tmpfact_order_shixiao;
CREATE TABLE IF NOT EXISTS dwd.tmpfact_order_shixiao (
   `date_key` bigint  COMMENT '日期维表键',
  `product_key` bigint  COMMENT '产品维表键',
  `org_key` bigint  COMMENT '组织架构维表建',
  `apply_no` STRING  COMMENT '订单编号',
  `order_create_date` TIMESTAMP  COMMENT '订单创建时间',
  `product_name` STRING  COMMENT '产品名称',
  `version` STRING  COMMENT '版本',
  `shenqing_amount` double  COMMENT '申请金额',
  `mqbc_time` TIMESTAMP  COMMENT '面签保存时间',
  `mianqian_date` TIMESTAMP  COMMENT '面签时间',
  `tjlr_date` TIMESTAMP  COMMENT '提交录入时间（变红时间）',
  `shencha_date` TIMESTAMP  COMMENT '审查时间',
  `bxqq_date` TIMESTAMP  COMMENT '保险起期时间',
  `fkzlts_date` TIMESTAMP  COMMENT '放款指令推送',
  `fsfkzl_date` TIMESTAMP  COMMENT '发送放款指令',
  `jkqx_date` TIMESTAMP  COMMENT '借款起息日（客户）',
  `daozhang_date` TIMESTAMP  COMMENT '贷款放款-到账时间',
  `dycj_date` TIMESTAMP  COMMENT '抵押出件时间',
  `dgszs_date` TIMESTAMP  COMMENT '到公司账上时间',
  `jiebao_date` TIMESTAMP  COMMENT '解保时间',
  `ccth_date` TIMESTAMP  COMMENT '初次退回时间',
  `baoshen_date` TIMESTAMP  COMMENT '报审日期',
  `bjtz_date` TIMESTAMP  COMMENT '补件通知日期',
  `djwcan_date` TIMESTAMP  COMMENT '点击外传按钮时间',
  `jgfhspjg_date` TIMESTAMP  COMMENT '机构返回审批结果时间',
  `hzjgfhfk_date` TIMESTAMP  COMMENT '合作机构返回放款时间',
  `etl_update_time` TIMESTAMP COMMENT '更新时间',
  `sales_user_name` STRING  COMMENT '渠道经理姓名',
  `product_id` STRING ,
  `mianqian_user` STRING  COMMENT '面签经办人',
  `sclrldsd_date` TIMESTAMP  COMMENT '首次录入捞单锁定时间',
  `bj_reason` STRING COMMENT '补件原因',
  `bujian_list` STRING,
  `ccbjwc_date` TIMESTAMP  COMMENT '初次补件完成时间',
  `zzbjwc_date` TIMESTAMP  COMMENT '最终补件完成时间',
  `lrtj_date` TIMESTAMP  COMMENT '录入提交时间',
  `lrjbr_name` STRING  COMMENT '录入经办人',
  `cfdeclr_date` TIMESTAMP  COMMENT '触发第二次录入时间',
  `fkclwt_date` TIMESTAMP  COMMENT '反馈材料问题时间',
  `tzlrwc_date` TIMESTAMP  COMMENT '通知录入完成时间',
  `lrld_date` TIMESTAMP  COMMENT '录入捞单时间',
  `lrwc_date` TIMESTAMP  COMMENT '录入完成时间',
  `lrwc_name` STRING  COMMENT '录入完成经办人',
  `baoshen_user` STRING  COMMENT '报审经办人',
  `shencha_result` STRING  COMMENT '审查结果',
  `shencha_opinion` STRING COMMENT '审查意见',
  `shencha_user` STRING  COMMENT '审查经办人',
  `scsd_date` TIMESTAMP  COMMENT '审查锁定时间',
  `sp_zlbc_date` TIMESTAMP  COMMENT '审批资料补充日期',
  `shenpi_date` TIMESTAMP  COMMENT '审批日期',
  `shenpi_result` STRING  COMMENT '审批结果',
  `shenpi_opinion` STRING COMMENT '审批意见',
  `shenpi_user` STRING  COMMENT '审批人员',
  `fjsp_flag` STRING  COMMENT '是否分级审批',
  `wczlts_date` TIMESTAMP  COMMENT '外传指令推送_处理时间',
  `wczlts_user` STRING  COMMENT '外传指令推送_经办人',
  `spzlwc_date` TIMESTAMP  COMMENT '审批资料外传/核保_处理时间',
  `spzlwc_user` STRING  COMMENT '审批资料外传/核保_经办人',
  `spjgqr_date` TIMESTAMP  COMMENT '审批结果确认_处理时间',
  `spjgqr_user` STRING  COMMENT '审批结果确认_经办人',
  `fkzlts_user` STRING  COMMENT '放款指令推送时间',
  `fsfkzl_user` STRING  COMMENT '发送放款指令经办人',
  `zjghsq_date` TIMESTAMP  COMMENT '资金归还申请_处理时间',
  `zjghsq_use` STRING  COMMENT '资金归还申请_经办人',
  `zjghqr_date` TIMESTAMP  COMMENT '资金归还确认_处理时间',
  `slq_dzsq_date` TIMESTAMP  COMMENT '垫资申请时间（赎楼前垫资）',
  `slq_dz_date` TIMESTAMP  COMMENT '垫资到账时间（赎楼前垫资）',
  `slq_gh_date` TIMESTAMP  COMMENT '归还时间（赎楼前垫资',
  `slq_dz_reason` STRING COMMENT '垫资原因（赎楼前垫资)',
  `dqgh_dzsq_date` TIMESTAMP  COMMENT '垫资申请时间（到期垫资）',
  `dqgh_dzdz_date` TIMESTAMP  COMMENT '垫资到账时间（到期垫资）',
  `dqgh_gh_date` TIMESTAMP  COMMENT '归还时间（到期垫资）',
  `dqgh_dz_reason` STRING COMMENT '垫资原因（赎楼前垫资)',
  `is_change_plat` string  COMMENT '是否更换平台（是/否）',
  `zjghqr_user` STRING  COMMENT '资金归还确认_经办人',
  `sldj_date` TIMESTAMP  COMMENT '赎楼登记时间',
  `lqzxzl_date` TIMESTAMP  COMMENT '领取注销资料时间',
  `zxdy_date` TIMESTAMP  COMMENT '注销抵押时间',
  `ghdj_date` TIMESTAMP  COMMENT '过户递件时间',
  `ghcj_date` TIMESTAMP  COMMENT '过户出件时间',
  `dydj_date` TIMESTAMP  COMMENT '抵押递件时间',
  `yhfk_date` TIMESTAMP  COMMENT '银行放款时间',
  `hptk_date` TIMESTAMP  COMMENT '还平台款时间',
  `gd_date` TIMESTAMP  COMMENT '归档时间',
  `hbzlwc_date` TIMESTAMP  COMMENT '核保资料外传时间',
  `cbd_date` TIMESTAMP  COMMENT '出保单时间',
  `partner_bank_name` STRING  COMMENT '合作银行'
);

WITH temp_new_loan AS (
	SELECT biz_loan_amount,house_no,
	row_number() over(PARTITION BY house_no ORDER BY apply_no) rank
	FROM  ods_bpms_biz_new_loan_common bnl
	WHERE house_no IS NOT NULL AND house_no !=''
)
,temp_order_matter_record AS (
	SELECT apply_no,matter_key,handle_time,if(handle_user_name='',null,handle_user_name)handle_user_name,create_time,
	row_number() over(PARTITION BY apply_no,matter_key ORDER BY CASE WHEN handle_time IS NULL THEN 1 ELSE 0 END ASC,handle_time ASC) rank1,
	row_number() over(PARTITION BY apply_no,matter_key ORDER BY CASE WHEN create_time IS NULL THEN 1 ELSE 0 END ASC,create_time ASC) rank2,
	row_number() over(PARTITION BY apply_no,matter_key ORDER BY CASE WHEN handle_time IS NULL THEN 1 ELSE 0 END ASC,handle_time DESC) rank3,
	row_number() over(PARTITION BY apply_no,matter_key ORDER BY CASE WHEN create_time IS NULL THEN 1 ELSE 0 END ASC,create_time DESC) rank4
	FROM ods.ods_bpms_biz_order_matter_record_common
--	where handle_time is not null
)
,temp_order_matter_record_sort AS (
	SELECT apply_no,
	min(CASE WHEN LOWER(matter_key)='interview' AND rank1=1 THEN CAST(handle_time AS STRING) END) interview_min_ht,
	min(CASE WHEN LOWER(matter_key)='interview' AND rank2=1 THEN CAST(create_time AS STRING) END) interview_min_ct,
	min(CASE WHEN LOWER(matter_key)='interview' AND rank2=1 THEN handle_user_name END) interview_handle_name,
	min(CASE WHEN LOWER(matter_key)='inputinfo' AND rank1=1 THEN CAST(handle_time AS STRING) END) inputinfo_min_ht,
	min(CASE WHEN LOWER(matter_key)='inputinfo' AND rank2=1 THEN CAST(create_time AS STRING) END) inputinfo_min_ct,
	min(CASE WHEN LOWER(matter_key)='inputinfo' AND rank2=1 THEN handle_user_name END) inputinfo_handle_name,
	min(CASE WHEN LOWER(matter_key)='applycheck' AND rank1=1 THEN CAST(handle_time AS STRING) END) applycheck_min_ht,
	min(CASE WHEN LOWER(matter_key)='applycheck' AND rank2=1 THEN handle_user_name END) applycheck_handle_name,
	min(CASE WHEN LOWER(matter_key)='uploadimg' AND rank1=1 THEN CAST(handle_time AS STRING) END) uploadimg_min_ht,
	min(CASE WHEN LOWER(matter_key)='uploadimg' AND rank2=1 THEN CAST(create_time AS STRING) END) uploadimg_min_ct,
	min(CASE WHEN LOWER(matter_key)='uploadimg' AND rank3=1 THEN CAST(handle_time AS STRING) END) uploadimg_max_ht,
	min(CASE WHEN LOWER(matter_key)='inputinfocomplete' AND rank1=1 THEN CAST(handle_time AS STRING) END) inputinfocomplete_min_ht,
	min(CASE WHEN LOWER(matter_key)='inputinfocomplete' AND rank2=1 THEN handle_user_name END) inputinfocomplete_handle_name,
	min(CASE WHEN LOWER(matter_key)='mancheck' AND rank1=1 THEN CAST(handle_time AS STRING) END) mancheck_min_ht,
	min(CASE WHEN LOWER(matter_key)='mancheck' AND rank2=1 THEN handle_user_name END) mancheck_min_hun,
	min(CASE WHEN LOWER(matter_key)='mancheck' AND rank3=1 THEN CAST(handle_time AS STRING) END) mancheck_max_ht,
	min(CASE WHEN LOWER(matter_key)='mancheck' AND rank4=1 THEN handle_user_name END) mancheck_max_hun,
	min(CASE WHEN LOWER(matter_key)='investigate' AND rank1=1 THEN CAST(handle_time AS STRING) END) investigate_min_ht,
	min(CASE WHEN LOWER(matter_key)='investigate' AND rank2=1 THEN handle_user_name END) investigate_min_hun,
	min(CASE WHEN LOWER(matter_key)='investigate' AND rank3=1 THEN CAST(handle_time AS STRING) END) investigate_max_ht,
	min(CASE WHEN LOWER(matter_key)='investigate' AND rank4=1 THEN handle_user_name END) investigate_max_hun,
	min(CASE WHEN LOWER(matter_key)='pushoutcommand' AND rank3=1 THEN CAST(handle_time AS STRING) END) pushoutcommand_max_ht,
	min(CASE WHEN LOWER(matter_key)='pushoutcommand' AND rank3=1 THEN handle_user_name END) pushoutcommand_handle_name,
	min(CASE WHEN LOWER(matter_key)='checkmarpush' AND rank3=1 THEN CAST(handle_time AS STRING) END) checkmarpush_max_ht,
	min(CASE WHEN LOWER(matter_key)='checkmarpush' AND rank3=1 THEN handle_user_name END) checkmarpush_handle_name,
	min(CASE WHEN LOWER(matter_key)='submitverifyapply' AND rank3=1 THEN CAST(handle_time AS STRING) END) submitverifyapply_max_ht,
	min(CASE WHEN LOWER(matter_key)='submitverifyapply' AND rank3=1 THEN handle_user_name END) submitverifyapply_handle_name,
	min(CASE WHEN LOWER(matter_key)='checkmarsend' AND rank3=1 THEN CAST(handle_time AS STRING) END) checkmarsend_max_ht,
	min(CASE WHEN LOWER(matter_key)='checkmarsend' AND rank3=1 THEN handle_user_name END) checkmarsend_handle_name,
	min(CASE WHEN LOWER(matter_key)='sendverifymaterial' AND rank1=1 THEN CAST(handle_time AS STRING) END) sendverifymaterial_min_ht,
	min(CASE WHEN LOWER(matter_key)='sendverifymaterial' AND rank3=1 THEN CAST(handle_time AS STRING) END) sendverifymaterial_max_ht,
	min(CASE WHEN LOWER(matter_key)='sendverifymaterial' AND rank3=1 THEN handle_user_name END) sendverifymaterial_handle_name,
	min(CASE WHEN LOWER(matter_key)='approvalresult' AND rank1=1 THEN CAST(handle_time AS STRING) END) approvalresult_min_ht,
	min(CASE WHEN LOWER(matter_key)='approvalresult' AND rank1=1 THEN handle_user_name END) approvalresult_handle_name,
	min(CASE WHEN LOWER(matter_key)='checkresensure' AND rank1=1 THEN CAST(handle_time AS STRING) END) checkresensure_min_ht,
	min(CASE WHEN LOWER(matter_key)='checkresensure' AND rank1=1 THEN handle_user_name END) checkresensure_handle_name,
	min(CASE WHEN LOWER(matter_key)='pushloancommand' AND rank1=1 THEN CAST(handle_time AS STRING) END) pushloancommand_min_ht,
	min(CASE WHEN LOWER(matter_key)='pushloancommand' AND rank1=1 THEN handle_user_name END) pushloancommand_handle_name,
	min(CASE WHEN LOWER(matter_key)='sendloancommand' AND rank1=1 THEN CAST(handle_time AS STRING) END) sendloancommand_min_ht,
	min(CASE WHEN LOWER(matter_key)='sendloancommand' AND rank1=1 THEN handle_user_name END) sendloancommand_handle_name,
	min(CASE WHEN LOWER(matter_key)='mortgageout' AND rank1=1 THEN CAST(handle_time AS STRING) END) mortgageout_min_ht,
	min(CASE WHEN LOWER(matter_key)='mortgageout_zz' AND rank1=1 THEN CAST(handle_time AS STRING) END) mortgageout_zz_min_ht,
	min(CASE WHEN LOWER(matter_key)='returnapply' AND rank1=1 THEN CAST(handle_time AS STRING) END) returnapply_min_ht,
	min(CASE WHEN LOWER(matter_key)='returnapply' AND rank1=1 THEN handle_user_name END) returnapply_handle_name,
	min(CASE WHEN LOWER(matter_key)='returnconfirm' AND rank1=1 THEN CAST(handle_time AS STRING) END) returnconfirm_min_ht,
	min(CASE WHEN LOWER(matter_key)='returnconfirm' AND rank1=1 THEN handle_user_name END) returnconfirm_handle_name,
	min(CASE WHEN LOWER(matter_key)='overinsurance' AND rank1=1 THEN CAST(handle_time AS STRING) END) overinsurance_min_ht,
	min(CASE WHEN LOWER(matter_key)='randommark' AND rank1=1 THEN CAST(handle_time AS STRING) END) randommark_min_ht,
	min(CASE WHEN LOWER(matter_key)='getcancelmaterial' AND rank1=1 THEN CAST(handle_time AS STRING) END) getcancelmaterial_min_ht,
	min(CASE WHEN LOWER(matter_key)='canclemortgage' AND rank1=1 THEN CAST(handle_time AS STRING) END) canclemortgage_min_ht,
	min(CASE WHEN LOWER(matter_key)='transferin' AND rank1=1 THEN CAST(handle_time AS STRING) END) transferin_min_ht,
	min(CASE WHEN LOWER(matter_key)='transferout' AND rank1=1 THEN CAST(handle_time AS STRING) END) transferout_min_ht,
	min(CASE WHEN LOWER(matter_key)='mortgagepass' AND rank1=1 THEN CAST(handle_time AS STRING) END) mortgagepass_min_ht,
	min(CASE WHEN LOWER(matter_key)='custreceivableconfirm' AND rank1=1 THEN CAST(handle_time AS STRING) END) custreceivableconfirm_min_ht,
	min(CASE WHEN LOWER(matter_key)='prefile' AND rank1=1 THEN CAST(handle_time AS STRING) END) prefile_min_ht
	FROM temp_order_matter_record
	where LOWER(matter_key) in ('interview', 'inputinfo', 'applycheck', 'uploadimg', 'inputinfocomplete', 'mancheck', 'investigate','pushoutcommand', 'checkmarpush'
	, 'submitverifyapply', 'submitverifyapply', 'checkmarsend', 'sendverifymaterial','approvalresult', 'checkresensure', 'pushloancommand', 'sendloancommand'
	, 'mortgageout', 'mortgageout_zz', 'returnapply', 'returnconfirm', 'overinsurance', 'randommark', 'getcancelmaterial', 'transferout', 'canclemortgage', 'transferin'
	, 'mortgagepass', 'custreceivableconfirm', 'prefile'  )
	GROUP BY apply_no
)
,temp_order_matter_record_compare AS (
	SELECT bomr.create_time,bomr.apply_no
	FROM ods_bpms_biz_order_matter_record_common bomr
	LEFT JOIN (SELECT * FROM temp_order_matter_record WHERE matter_key='ApplyCheck' AND rank1=1) omrac ON omrac.apply_no = bomr.apply_no
          WHERE omrac.apply_no=bomr.apply_no AND bomr.matter_key='UploadImg'
          AND bomr.create_time < omrac.handle_time
)
,temp_order_matter_record_bim AS (
	SELECT
	bomr.apply_no
	,bomr.matter_key
	,bomr.create_time
	,row_number() over(PARTITION BY bomr.apply_no,bomr.matter_key ORDER BY bomr.create_time) rank
	FROM ods_bpms_biz_order_matter_record_common bomr
	LEFT JOIN (
		SELECT send_partner_ding_msg_time,apply_no FROM ods_bpms_biz_isr_mixed_common bim
		WHERE bim.is_partner_ding_msg_sent = '1'
		)bim ON bomr.apply_no=bim.apply_no
    where bim.send_partner_ding_msg_time  IS NOT NULL AND bomr.create_time >= bim.send_partner_ding_msg_time
)
,temp_order_flow AS (
	SELECT apply_no,flow_type,handle_time,handle_user_name,
	row_number() over(PARTITION BY apply_no,flow_type ORDER BY handle_time asc) rank
	FROM ods_bpms_biz_order_flow_common bof
)
,temp_order_flow_sort AS (
	SELECT apply_no,
	min(CASE WHEN LOWER(flow_type)='mq_type' AND rank=1 THEN CAST(handle_time AS STRING) END) mq_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='mq_type' AND rank=1 THEN (handle_user_name) END) mq_type_min_hun,
	min(CASE WHEN LOWER(flow_type)='sldj_type' AND rank=1 THEN CAST(handle_time AS STRING) END) jb_pc_min_ht,
	min(CASE WHEN LOWER(flow_type)='sldj_type' AND rank=1 THEN CAST(handle_time AS STRING) END) sldj_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='qzjzxcl_type' AND rank=1 THEN CAST(handle_time AS STRING) END) qzjzxcl_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='zxdy_type' AND rank=1 THEN CAST(handle_time AS STRING) END) zxdy_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='gh_type' AND rank=1 THEN CAST(handle_time AS STRING) END) gh_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='qxz_type' AND rank=1 THEN CAST(handle_time AS STRING) END) qxz_type_min_ht,
	min(CASE WHEN LOWER(flow_type)='bldy_type' AND rank=1 THEN CAST(handle_time AS STRING) END) bldy_type_min_ht
	FROM temp_order_flow
	where LOWER(flow_type) in ('mq_type', 'sldj_type','qzjzxcl_type', 'zxdy_type' , 'gh_type', 'qxz_type', 'bldy_type')
	GROUP BY apply_no
)
,temp_check_opinion AS (
	SELECT ASSIGN_TIME_,PROC_INST_ID_,TASK_KEY_,COMPLETE_TIME_,STATUS_,OPINION_,AUDITOR_NAME_,TASK_NAME_,
	row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN ASSIGN_TIME_ IS NULL OR ASSIGN_TIME_ = '' THEN 1 ELSE 0 END ASC,ASSIGN_TIME_ ASC) rank1,
	row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN COMPLETE_TIME_ IS NULL OR COMPLETE_TIME_ = '' THEN 1 ELSE 0 END ASC,COMPLETE_TIME_ ASC) rank2,
	row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN create_time_ IS NULL OR create_time_ = '' THEN 1 ELSE 0 END ASC,create_time_ ASC) rank3,
	row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN COMPLETE_TIME_ IS NULL OR COMPLETE_TIME_ = '' THEN 1 ELSE 0 END ASC,COMPLETE_TIME_ DESC) rank4,
	row_number() over(PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN create_time_ IS NULL OR create_time_ = '' THEN 1 ELSE 0 END ASC,create_time_ DESC) rank5
	FROM ods_bpms_bpm_check_opinion_common
	where LOWER(TASK_KEY_) in ('inputinfo', 'usertask2', 'usertask3', 'usertask4', 'mancheck', 'investigate', 'usertask7', 'usertask5', 'usertask1', 'usertask6')
)
,temp_check_opinion_sort AS (
	SELECT PROC_INST_ID_,
	min(CASE WHEN LOWER(TASK_KEY_)='inputinfo' AND rank1=1 THEN (CAST(ASSIGN_TIME_ AS STRING)) END) inputinfo_min_at,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask2' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask2_min_ct,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask2' AND rank3=1 THEN (AUDITOR_NAME_) END) usertask2_min_an ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask3_min_ct,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND rank3=1 THEN (AUDITOR_NAME_) END) usertask3_min_an ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND rank3=1 THEN (STATUS_) END) usertask3_min_status ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND rank3=1 THEN (OPINION_) END) usertask3_min_opinion ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND STATUS_= "manual_end" AND OPINION_  LIKE "%捞取%" AND rank3=1
		THEN (CASE WHEN OPINION_ NOT LIKE "%系统自动为%" THEN SUBSTR(OPINION_,0, LOCATE('已', OPINION_) - 1)
                   ELSE SUBSTR(OPINION_, 6, LOCATE('捞', OPINION_) -6)
                   END)
        END) usertask3_opinion_sub,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask3' AND rank5=1 THEN (AUDITOR_NAME_) END) usertask3_max_an ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask4_min_ct,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank3=1 THEN (AUDITOR_NAME_) END) usertask4_min_an ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank3=1 THEN (STATUS_) END) usertask4_min_status ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank3=1 THEN (OPINION_) END) usertask4_min_opinion ,
	min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND STATUS_= "manual_end" AND OPINION_  LIKE "%捞取%" AND rank3=1
		THEN (CASE WHEN OPINION_ NOT LIKE "%系统自动为%" THEN SUBSTR(OPINION_,0, LOCATE('已', OPINION_) - 1)
                   ELSE SUBSTR(OPINION_, 6, LOCATE('捞', OPINION_) -6)
                   END)
        END) usertask4_opinion_sub,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank4=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask4_max_ct,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank5=1 THEN (STATUS_) END) usertask4_max_status,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank5=1 THEN (OPINION_) END) usertask4_max_opinion,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask4' AND rank5=1 THEN (AUDITOR_NAME_) END) usertask4_max_an ,
    min(CASE WHEN LOWER(TASK_KEY_)='mancheck' AND rank5=1 THEN (STATUS_) END) mancheck_max_status,
    min(CASE WHEN LOWER(TASK_KEY_)='mancheck' AND rank5=1 THEN (OPINION_) END) mancheck_max_opinion,
	min(CASE WHEN LOWER(TASK_KEY_)='mancheck' AND rank3=1 THEN (STATUS_) END) mancheck_min_status ,
	min(CASE WHEN LOWER(TASK_KEY_)='mancheck' AND rank3=1 THEN (OPINION_) END) mancheck_min_opinion ,
    min(CASE WHEN LOWER(TASK_KEY_)='mancheck' AND STATUS_= "manual_end" AND OPINION_  LIKE "%捞取%" AND rank3=1
        THEN (CASE WHEN OPINION_ NOT LIKE "%系统自动为%" THEN SUBSTR(OPINION_,0, LOCATE('已', OPINION_) - 1)
                   ELSE SUBSTR(OPINION_, 6, LOCATE('捞', OPINION_) -6)
                   END)
        END) mancheck_opinion_sub,
    min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND rank1=1 THEN (CAST(ASSIGN_TIME_ AS STRING)) END) investigate_min_at,
	min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND rank3=1 THEN (STATUS_) END) investigate_min_status,
	min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND rank3=1 THEN (OPINION_) END) investigate_min_opinion ,
	min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND STATUS_= "manual_end" AND OPINION_  LIKE "%捞取%" AND rank3=1
		THEN (CASE WHEN OPINION_ NOT LIKE "%系统自动为%" THEN SUBSTR(OPINION_,0, LOCATE('已', OPINION_) - 1)
              ELSE SUBSTR(OPINION_, 6, LOCATE('捞', OPINION_) -6)
              END)
         END) investigate_opinion_sub,
    min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND rank5=1 THEN (STATUS_) END) investigate_max_status,
    min(CASE WHEN LOWER(TASK_KEY_)='investigate' AND rank5=1 THEN (OPINION_) END) investigate_max_opinion,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask7' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask7_min_ct,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask7' AND rank4=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask7_max_ct,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask7' AND rank4=1 THEN (AUDITOR_NAME_) END) usertask7_max_an ,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask5' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask5_min_ct,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask5' AND rank2=1 THEN (AUDITOR_NAME_) END) usertask5_min_an ,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask1' AND TASK_NAME_ = "资金归还申请" AND rank2=1 THEN (AUDITOR_NAME_) END) usertask1_zjghsq_min_an,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask2' AND TASK_NAME_ = "资金归还确认" AND rank2=1 THEN (AUDITOR_NAME_) END) usertask2_zjghqr_min_an,
    min(CASE WHEN LOWER(TASK_KEY_)='usertask6' AND STATUS_='agree' AND rank2=1 THEN (CAST(COMPLETE_TIME_ AS STRING)) END) usertask6_min_ct
	FROM temp_check_opinion GROUP BY PROC_INST_ID_
)
,temp_missing_materials AS (
	SELECT apply_no,node_id,create_time,material_name,group_rev,material_code,
	row_number() over(PARTITION BY apply_no,node_id ORDER BY
		CASE WHEN create_time IS NULL OR create_time = '' THEN 1 ELSE 0 END ASC, create_time) rankmin,
	row_number() over(PARTITION BY apply_no,node_id ORDER BY
		CASE WHEN create_time IS NULL OR create_time = '' THEN 1 ELSE 0 END ASC,create_time DESC) rankmax
	FROM ods_bpms_biz_missing_materials
)
,temp_missing_materials_compare AS (
	SELECT bmm.apply_no,bmm.material_name,material_code
	FROM ods_bpms_biz_missing_materials bmm
	LEFT JOIN (SELECT * FROM temp_order_matter_record bomr WHERE bomr.matter_key='ApplyCheck' AND rank1=1)bomr ON bmm.apply_no=bomr.apply_no
	WHERE bmm.group_rev = 1 AND bmm.material_code <> "000000"
        AND bmm.create_time < bomr.handle_time
)
,temp_fd_advance AS (
	SELECT apply_date,fin_date,adv_reason,apply_no,adv_type,
	row_number() over(PARTITION BY apply_no,adv_type ORDER BY apply_date DESC) rank1,
	row_number() over(PARTITION BY apply_no,adv_type ORDER BY fin_date DESC) rank2
	FROM ods_bpms_fd_advance WHERE delete_flag <> '1'
)
,temp_fd_advance_sort AS (
	SELECT apply_no,
	min(CASE WHEN LOWER(adv_type)='flooradvance' AND rank1=1 THEN (CAST(apply_date AS STRING)) END) flooradvance_max_ad,
	min(CASE WHEN LOWER(adv_type)='flooradvance' AND rank2=1 THEN (CAST(fin_date AS STRING)) END) flooradvance_max_fd,
	min(CASE WHEN LOWER(adv_type)='flooradvance' AND rank1=1 THEN (CAST(adv_reason AS STRING)) END) flooradvance_adv_reason,
	min(CASE WHEN LOWER(adv_type)='expireadvance' AND rank1=1 THEN (CAST(apply_date AS STRING)) END) expireadvance_max_ad,
	min(CASE WHEN LOWER(adv_type)='expireadvance' AND rank2=1 THEN (CAST(fin_date AS STRING)) END) expireadvance_max_fd,
	min(CASE WHEN LOWER(adv_type)='expireadvance' AND rank1=1 THEN (CAST(adv_reason AS STRING)) END) expireadvance_adv_reason
	FROM temp_fd_advance GROUP BY apply_no
)
,temp_fd_advance_ret AS (
	SELECT far.fin_date,far.apply_id,fa.apply_no,fa.adv_type,
	row_number() over(PARTITION BY fa.apply_no,fa.adv_type ORDER BY far.fin_date DESC)rank
	FROM ods_bpms_fd_advance_ret far
	LEFT JOIN (SELECT * FROM ods_bpms_fd_advance WHERE delete_flag <> '1') fa ON  far.apply_id = fa.id
	where far.fin_date is not null
)
,ids_order_shixiao AS (


	-- explain
SELECT
 bao.apply_no
 ,bao.create_time order_create_date
 ,to_date(bao.create_time) order_create_date_d
 ,bao.sales_user_id
 ,bao.sales_user_name
 ,bao.branch_id
 ,bao.sales_branch_id
 ,CASE WHEN bao.product_name = "及时贷（非交易））" THEN "及时贷（非交易提放）"
       WHEN bao.product_name = "提放保-无赎楼" THEN "提放保（无赎楼）"
       WHEN bao.product_name = "提放保-有赎楼" THEN "提放保（有赎楼）"
       WHEN bao.product_name = "提放保(无赎楼)" THEN "提放保（无赎楼）"
  ELSE bao.product_name
 END product_name
 ,CASE WHEN bao.product_version IS NULL OR bao.product_version = ""
     THEN
       CASE WHEN bao.product_id = "NSL-JYB371" THEN "v1.0"
          WHEN bao.product_id = "NSL-JYB377" THEN "v1.0"
          WHEN bao.product_id = "NSL-JYB755" THEN "v1.0"
          WHEN bao.product_id = "NSL-TFB371" THEN "v1.0"
          WHEN bao.product_id = "SL-TFB371" THEN "v1.0"
          WHEN bao.product_id = "SL-JYB371" THEN "v1.5"
          WHEN bao.product_id = "SL-JYB374" THEN "v1.5"
     END
 ELSE bao.product_version
 END VERSION

 ,bao.product_id
 ,bao.PROC_DEF_KEY_
 ,CASE WHEN bao.product_name LIKE "%提放保%"
       THEN nvl(nvl(bim.bank_loan_amount, bnlh.biz_loan_amount) -- "v2.5", "2.0"
        ,bnla.biz_loan_amount) -- "v1.5", "v1.0"

       WHEN bao.product_name LIKE "%交易保%"
       THEN nvl(bnlh.biz_loan_amount,bnla.biz_loan_amount)

    WHEN (bao.product_name LIKE '%买付保%')
         THEN bfs.guarantee_amount
    WHEN (bao.product_name LIKE "%及时贷%" AND bao.product_name NOT LIKE "%贷款服务%")  OR bao.product_name = "大道房抵贷"
         THEN bfs.borrowing_amount
    WHEN ( bao.product_name = "及时贷（贷款服务）"
      OR bao.product_name = "大道快贷（贷款服务）"
      OR bao.product_name = "大道易贷（贷款服务）"
      OR bao.product_name = "限时贷"
      OR bao.product_name LIKE "%拍卖保%"
      OR bao.product_name = "大道按揭"
        )
         THEN (bnlh.biz_loan_amount)

     ELSE NULL
    END shenqing_amount

,nvl(CAST(bimin.interview_save_time AS STRING)
     ,nvl(tomrs.interview_min_ht, tofs.mq_type_min_ht)
     ) mqbc_time -- 面签保存时间

,nvl(tomrs.interview_min_ht, tofs.mq_type_min_ht
     ) mianqian_date -- 面签时间

,nvl(tomrs.interview_handle_name,tofs.mq_type_min_hun
     ) mianqian_user -- 面签经办人


,IF(bim.apply_order_attach_complete_time > CAST(tomrs.inputinfo_min_ct AS TIMESTAMP)
    ,bim.apply_order_attach_complete_time
    ,CAST(tomrs.inputinfo_min_ct AS TIMESTAMP)
    ) tjlr_date -- '提交录入时间（变红时间）'

,tcos.inputinfo_min_at sclrldsd_date -- 首次录入捞单锁定时间

, bim.materials_upload_comment bj_reason -- '补件原因',

,CASE WHEN (tomrs.applycheck_min_ht) IS NULL
  THEN bmm.material_name
  ELSE bmmc.material_name
  END bujian_list -- '补件清单'

,CASE WHEN (tomrs.applycheck_min_ht) IS NULL OR (tomrs.applycheck_min_ht)=''
      THEN (tomrs.uploadimg_min_ct)
      ELSE CAST(omruiacc.create_time AS STRING)
 END ccth_date -- 初次退回时间

,CASE WHEN (tomrs.applycheck_min_ht) IS NULL
     THEN (tomrs.uploadimg_min_ht)
     ELSE CAST(omruiacminh.handle_time AS STRING)
     END ccbjwc_date  -- '初次补件完成时间'

,CASE WHEN (tomrs.applycheck_min_ht) IS NULL
     THEN (tomrs.uploadimg_max_ht)
     ELSE CAST(omruiacmaxh.handle_time AS STRING)
     END zzbjwc_date  -- '最终补件完成时间'

,CASE WHEN bao.product_version NOT IN ('v1', 'v1.5')
     THEN (tomrs.inputinfo_min_ht)
     WHEN bao.product_version IN ('v1', 'v1.5')
     THEN (tcos.usertask2_min_ct)

    END lrtj_date -- 录入提交时间

,CASE WHEN bao.product_version NOT IN ('v1', 'v1.5')
         THEN (tomrs.inputinfo_handle_name)
         WHEN bao.product_version IN ('v1', 'v1.5')
         THEN (tcos.usertask2_min_an)
    END lrjbr_name -- 录入经办人

,(bimipdms.send_partner_ding_msg_time) cfdeclr_date -- 触发第二次录入时间

, (omrbim.create_time)  fkclwt_date -- '反馈材料问题时间'

--, (bim.moo_inputcomple_time) tzlrwc_date -- '通知录入完成时间'
, (imd.min_update_time) tzlrwc_date -- '通知录入完成时间'

, (tcos.inputinfo_min_at) lrld_date -- 录入捞单时间

, (tomrs.inputinfocomplete_min_ht) lrwc_date -- 录入完成时间

, (tomrs.inputinfocomplete_handle_name) lrwc_name -- 录入完成经办人

, CASE WHEN bao.product_id IN ("NSL-TFB371", "SL-TFB371")  -- 郑州提放保
    THEN (tcos.usertask3_min_ct)

    WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tcos.usertask2_min_ct)

    WHEN bao.product_version IN ('v2.0', 'v2.5')
    THEN (tomrs.applycheck_min_ht)

END  baoshen_date -- 报审日期


, CASE WHEN bao.product_id IN ("NSL-TFB371", "SL-TFB371")  -- 郑州提放保
    THEN (tcos.usertask3_min_an)

    WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tcos.usertask2_min_an)

    WHEN bao.product_version IN ('v2.0', 'v2.5')
    THEN (tomrs.applycheck_handle_name)


END  baoshen_user -- 报审经办人



,CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_min_ct)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask3_min_ct)

      WHEN bao.product_version = 'v2.0'
      THEN (tomrs.mancheck_min_ht)

      WHEN bao.product_version = 'v2.5'
      THEN (tomrs.investigate_min_ht)

      ELSE NULL

 END shencha_date

, CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_min_status)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask3_min_status)

      WHEN bao.product_version = 'v2.0'
      THEN (tcos.mancheck_min_status)

      WHEN bao.product_version = 'v2.5'
      THEN (tcos.investigate_min_status)

  END  shencha_result -- 审查结果

  , CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_min_opinion)
      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask3_min_opinion)
      WHEN bao.product_version = 'v2.0'
      THEN (tcos.mancheck_min_opinion)
      WHEN bao.product_version = 'v2.5'
      THEN (tcos.investigate_min_opinion)

  END  shencha_opinion -- 审查意见

, CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN
          nvl(
          (tcos.usertask4_opinion_sub)
          ,(tcos.usertask4_min_an)
          )

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN
          nvl(
          (tcos.usertask3_opinion_sub)
          ,(tcos.usertask3_min_an)
          )


      WHEN bao.product_version = 'v2.0'
      THEN
          nvl(
          (tcos.mancheck_opinion_sub)
          ,(tomrs.mancheck_min_hun)
          )

      WHEN bao.product_version = 'v2.5'
      THEN
          nvl(
          (tcos.investigate_opinion_sub)
          ,(tomrs.investigate_min_hun)
          )

      ELSE NULL

 END shencha_user -- 审查经办人

, (tcos.investigate_min_at) scsd_date -- 审查锁定时间


, (tmmmin.create_time) bjtz_date  -- 补件通知日期

, (tmmmax.create_time) sp_zlbc_date  -- 审批资料补充日期

,CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_max_ct)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask4_max_ct)

      WHEN bao.product_version = 'v2.0'
      THEN (tomrs.mancheck_max_ht)

      WHEN bao.product_version = 'v2.5'
      THEN nvl((tomrs.mancheck_max_ht)
                 ,(tomrs.investigate_max_ht))

      ELSE NULL

 END shenpi_date -- 审批日期

, CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_max_status)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask4_max_status)

      WHEN bao.product_version = 'v2.0'
      THEN (tcos.mancheck_max_status)

      WHEN bao.product_version = 'v2.5'
      THEN nvl((tcos.mancheck_max_status),
		(tcos.investigate_max_status)
          )

  END  shenpi_result -- 审批结果

, CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_max_opinion)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask4_max_opinion)

      WHEN bao.product_version = 'v2.0'
      THEN (tcos.mancheck_max_opinion)

      WHEN bao.product_version = 'v2.5'
      THEN nvl((tcos.mancheck_max_opinion),
          (tcos.investigate_max_opinion)
          )

  END  shenpi_opinion -- 审批意见

,CASE WHEN bao.product_id IN ('NSL-TFB371', 'SL-TFB371')
      THEN (tcos.usertask4_max_an)

      WHEN bao.product_version IN ('v1.0', 'v1.5')
      THEN (tcos.usertask3_max_an)

      WHEN bao.product_version = 'v2.0'
      THEN (tomrs.mancheck_max_hun)

      WHEN bao.product_version = 'v2.5'
      THEN nvl((tomrs.mancheck_max_hun),
                (tomrs.investigate_max_hun)
           )

      ELSE NULL

 END shenpi_user -- 审批人员

,IF((tomrs.mancheck_max_ht) IS NOT NULL , '是', '否') fjsp_flag -- 是否分级审批

,CASE WHEN bao.product_version = 'v2.5'
     THEN (tomrs.pushoutcommand_max_ht)

     WHEN bao.product_version = 'v2.0' AND bao.product_name LIKE "%及时贷%"
     THEN (tomrs.checkmarpush_max_ht)

     WHEN bao.product_version = 'v2.0' AND (bao.product_name LIKE "%交易保%" OR bao.product_name LIKE "%提放保%")
     THEN (tomrs.submitverifyapply_max_ht)
END wczlts_date -- 外传指令推送_处理时间

,CASE WHEN bao.product_version = 'v2.5'
     THEN (tomrs.pushoutcommand_handle_name)

     WHEN bao.product_version = 'v2.0' AND bao.product_name LIKE "%及时贷%"
     THEN (tomrs.checkmarpush_handle_name)

     WHEN bao.product_version = 'v2.0' AND (bao.product_name LIKE "%交易保%" OR bao.product_name LIKE "%提放保%")
     THEN (tomrs.submitverifyapply_handle_name)
END wczlts_user -- 外传指令推送_经办人

 ,(bpr.min_create_time)  djwcan_date -- 点击外传按钮时间

, CASE WHEN bao.product_version IN ('v2.5', 'v2.0') AND bao.product_name LIKE '%及时贷%'
       THEN (tomrs.checkmarsend_max_ht)

       WHEN bao.product_version IN ('v2.5', 'v2.0') AND (bao.product_name LIKE '%交易保%' OR bao.product_name LIKE '%提放保%' OR bao.product_name LIKE '%买付保%')
       THEN (tomrs.sendverifymaterial_max_ht)

       WHEN bao.product_version IN ('v1.5', 'v1.0')
       THEN (tcos.usertask7_max_ct)

END  spzlwc_date -- 审批资料外传/核保_处理时间

, CASE WHEN bao.product_version IN ('v2.5', 'v2.0') AND bao.product_name LIKE '%及时贷%'
       THEN (tomrs.checkmarsend_handle_name)

       WHEN bao.product_version IN ('v2.5', 'v2.0') AND (bao.product_name LIKE '%交易保%' OR bao.product_name LIKE '%提放保%' OR bao.product_name LIKE '%买付保%')
       THEN (tomrs.sendverifymaterial_handle_name)

       WHEN bao.product_version IN ('v1.5', 'v1.0')
       THEN (tcos.usertask7_max_an)

END spzlwc_user -- 审批资料外传/核保_经办人

, IF(nvl(tomrs.approvalresult_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING))<nvl(tomrs.checkresensure_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING)),
	tomrs.approvalresult_min_ht,tomrs.checkresensure_min_ht) spjgqr_date -- 审批结果确认_处理时间

, IF(nvl(tomrs.approvalresult_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING))<nvl(tomrs.checkresensure_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING)),
	tomrs.approvalresult_handle_name,tomrs.checkresensure_handle_name) spjgqr_user -- 审批结果确认_经办人

, CASE WHEN bao.partner_insurance_name IN ("蓝海投资", "挖财", "中广核富盈")
      THEN (bprza.min_update_time)
      ELSE (bprtg.min_update_time)
 END jgfhspjg_date -- 机构返回审批结果时间


,(min_policy_start) bxqq_date

,(tomrs.pushloancommand_min_ht) fkzlts_date  -- '放款指令推送时间',

,(pushloancommand_handle_name) fkzlts_user  -- '放款指令推送经办人',

,CASE WHEN bao.product_version IN ("v1.0", 'v1.5')
    THEN CAST(tcos.usertask5_min_ct AS TIMESTAMP)

    WHEN bao.product_name = "大道房抵贷"
    THEN (bprl.min_create_time)

    ELSE CAST(tomrs.sendloancommand_min_ht AS TIMESTAMP)

 END fsfkzl_date  -- 发送放款指令时间'

,bco_ex_wx.sendloancommand_user_name fsfkzl_user  -- 发送放款指令经办人'


 ,IF(bao.product_name = "大道房抵贷"
      , IF(bfs.platform_value_date = '', NULL, bfs.platform_value_date)
      , IF(borrowing_value_date = '', NULL, CAST(borrowing_value_date AS STRING))) jkqx_date -- 借款起息日

,(bprls.min_update_time) hzjgfhfk_date -- 合作机构返回放款时间

 ,CASE WHEN bao.product_id IN ("NSL-TFB371", "SL-TFB371") OR bao.product_version IN ("v1.0", "v1.5")
      THEN bim.arrival_time

      WHEN bao.product_name LIKE "%及时贷%" AND bao.product_name NOT LIKE "%贷款服务%"
      THEN (cfm.max_con_funds_time)

      ELSE CAST(tomrs.sendloancommand_min_ht AS TIMESTAMP)

 END daozhang_date -- 放款指令推送时间


,IF(if(tomrs.mortgageout_min_ht='' or tomrs.mortgageout_min_ht is null,date_format(CURRENT_TIMESTAMP(),'Y-M-d H:m:s'),tomrs.mortgageout_min_ht)
    <if(tomrs.mortgageout_zz_min_ht='' or tomrs.mortgageout_zz_min_ht is null,date_format(CURRENT_TIMESTAMP(),'Y-M-d H:m:s'),tomrs.mortgageout_zz_min_ht),
	tomrs.mortgageout_min_ht,tomrs.mortgageout_zz_min_ht) dycj_date -- 抵押出件时间

,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (bffi.min_to_company_account_time)

    ELSE bfs.borrowing_due_date
END dgszs_date

,CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  if(tomrs.returnapply_min_ht='' or tomrs.returnapply_min_ht is null,bco_ex_wx.returnapply_time,cast(tomrs.returnapply_min_ht as timestamp))

  WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  zjghsq.complete_time_

END zjghsq_date -- 资金归还申请_处理时间

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  nvl(tomrs.returnapply_handle_name,bco_ex_wx.returnapply_user_name)

   WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  nvl(tcos.usertask1_zjghsq_min_an,bco_ex_wx.returnapply_user_name)

END zjghsq_use  -- 资金归还申请_经办人

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  if(tomrs.returnconfirm_min_ht='' or tomrs.returnconfirm_min_ht is null,bco_ex_wx.returnconfirm_time,cast(tomrs.returnconfirm_min_ht as timestamp))

   WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  zjghqr.complete_time_

END zjghqr_date -- 资金归还确认_处理时间

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
   nvl(tomrs.returnconfirm_handle_name,bco_ex_wx.returnconfirm_user_name)

   WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  nvl(tcos.usertask2_zjghqr_min_an,bco_ex_wx.returnconfirm_user_name)

END zjghqr_user -- 资金归还确认_经办人

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tfas.flooradvance_max_ad)
  END slq_dzsq_date -- 垫资申请时间（赎楼前垫资）

, CASE WHEN bao.product_version IN ("v1.0", "v1.5") THEN
  (bffi.max_funded_time)

  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  CAST(tfas.flooradvance_max_fd AS TIMESTAMP)

 END slq_dz_date -- 垫资到账时间（赎楼前垫资）

, CASE WHEN bao.product_version IN ("v1.0", "v1.5") THEN
  IF((bffi.max_funded_time) IS NOT NULL
    ,(bimin.max_arrival_time)
    ,NULL )

  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (farfa.fin_date)

 END slq_gh_date -- 归还时间（赎楼前垫资)


, CASE
  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tfas.flooradvance_adv_reason)

 END slq_dz_reason -- 垫资原因（赎楼前垫资)

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tfas.expireadvance_max_ad)

  END dqgh_dzsq_date -- 垫资申请时间（到期垫资）

, CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tfas.expireadvance_max_fd)

  WHEN bao.product_version IN ("v1.0", "v1.5") THEN
  IF((bffi.has_funded_repayment) = 'Y'
    ,CAST(cct.max_trans_day AS STRING)
    , NULL )

END dqgh_dzdz_date -- 垫资到账时间（到期垫资）


, CASE WHEN bao.product_version IN ("v1.0", "v1.5") THEN
  IF(IF(IF((bffi.has_funded_repayment) = 'Y'
        ,(cct.max_trans_day)
        , NULL ) IS NOT NULL
        ,(bimin.max_arrival_time)
        ,NULL ) IS NOT NULL
    ,(bffi.max_to_company_account_time)
    ,NULL)

  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (farea.fin_date)

 END dqgh_gh_date -- 归还时间（到期垫资）

, CASE
  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tfas.expireadvance_adv_reason)

 END dqgh_dz_reason -- 垫资原因（赎楼前垫资)

, IF(bco.num >0,'是','否') is_change_plat  -- '是否更换平台（是/否）'

,bco_ex_wx.overinsurance_time jiebao_date

,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.sldj_type_min_ht)

    ELSE (tomrs.randommark_min_ht)

  END sldj_date -- 赎楼登记时间

,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.qzjzxcl_type_min_ht)

    ELSE (tomrs.getcancelmaterial_min_ht)

  END lqzxzl_date -- 领取注销资料时间

  ,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.zxdy_type_min_ht)

    ELSE (tomrs.canclemortgage_min_ht)

  END zxdy_date -- 注销抵押时间

  ,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.gh_type_min_ht)

    ELSE (tomrs.transferin_min_ht)

  END ghdj_date -- 过户递件时间

  ,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.qxz_type_min_ht)

    ELSE (tomrs.transferout_min_ht)

  END ghcj_date -- 过户出件时间

  ,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (tofs.bldy_type_min_ht)

    ELSE (tomrs.mortgagepass_min_ht)

  END dydj_date -- 抵押递件时间


 ,CASE WHEN bao.product_version IN ('v1.5', 'v1.0')
    THEN (bimin.min_arrival_time)

    WHEN bao.product_version IN ('v2.5', 'v2.0')AND (bao.product_name LIKE '%保%')
  THEN(cfm.min_con_funds_time)

    ELSE (bimin.min_bank_loan_time)

  END yhfk_date -- 银行放款时间

 , CASE WHEN bao.product_version IN ("v2.0", "v2.5") AND (bao.product_name NOT LIKE '%保%') THEN
  IF(nvl(tomrs.sendverifymaterial_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING))<nvl(tomrs.custreceivableconfirm_min_ht,CAST(CURRENT_TIMESTAMP()AS STRING)),
	tomrs.mortgageout_min_ht,tomrs.mortgageout_zz_min_ht)

   WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  CAST(min_return_funded_time AS STRING)

  END hptk_date -- 还平台款时间

  ,CASE WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  (tcos.usertask6_min_ct)

  WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tomrs.prefile_min_ht)

  END  gd_date -- 归档时间

  , CASE WHEN bao.product_version IN ("v2.0", "v2.5") THEN
  (tomrs.sendverifymaterial_min_ht)

   WHEN bao.product_version IN ('v1.0', 'v1.5') THEN
  (tcos.usertask7_min_ct)
  END hbzlwc_date -- 核保资料外传时间

,(bii.min_receive_policy_time)
     cbd_date -- 出保单时间

,partner_bank_name -- 合作银行


FROM  (SELECT * FROM ods_bpms_biz_apply_order_common WHERE apply_no !='') bao
LEFT JOIN ods_bpms_biz_fee_summary bfs ON bao.apply_no = bfs.apply_no
LEFT JOIN ods_bpms_biz_isr_mixed_common bim ON bao.apply_no = bim.apply_no
LEFT JOIN (SELECT * FROM temp_new_loan WHERE rank =1)bnlh ON bao.house_no = bnlh.house_no
LEFT JOIN (select * from ods_bpms_biz_new_loan_common where rn =1) bnla ON bao.apply_no = bnla.apply_no
LEFT JOIN (SELECT MIN(interview_save_time) AS interview_save_time,MAX(arrival_time)AS max_arrival_time,MIN(arrival_time)AS min_arrival_time,MIN(bank_loan_time)AS min_bank_loan_time,apply_no FROM ods_bpms_biz_isr_mixed_common GROUP BY apply_no)bimin ON bao.apply_no = bimin.apply_no
LEFT JOIN temp_order_matter_record_sort tomrs ON bao.apply_no = tomrs.apply_no
LEFT JOIN temp_check_opinion_sort tcos ON tcos.PROC_INST_ID_ = bao.flow_instance_id
LEFT JOIN temp_order_flow_sort tofs ON bao.apply_no = tofs.apply_no
LEFT JOIN (SELECT min(material_name) AS material_name,apply_no FROM temp_missing_materials_compare GROUP BY apply_no)bmmc ON bao.apply_no = bmmc.apply_no
LEFT JOIN (SELECT min(bmm.material_name) AS material_name,bmm.apply_no FROM (SELECT * FROM temp_missing_materials WHERE group_rev = 1 AND material_code <> "000000")bmm GROUP BY bmm.apply_no)bmm ON bao.apply_no = bmm.apply_no
LEFT JOIN (SELECT bomr.apply_no,bomr.create_time
           FROM (SELECT * FROM temp_order_matter_record WHERE matter_key='UploadImg' AND rank2=1 ) bomr
           LEFT JOIN(
                SELECT * FROM temp_order_matter_record WHERE  matter_key='ApplyCheck' AND rank1=1
                )bomr2 ON bomr.apply_no=bomr2.apply_no
           WHERE bomr.create_time < bomr2.handle_time
	)omruiacc ON bao.apply_no =omruiacc.apply_no
LEFT JOIN (SELECT bomr.apply_no,bomr.handle_time
           FROM(
            SELECT * FROM temp_order_matter_record WHERE matter_key='UploadImg' AND rank1=1
            )bomr
           LEFT JOIN (
                SELECT * FROM temp_order_matter_record WHERE  matter_key='ApplyCheck' AND rank1=1
                )bomr2 ON bomr.apply_no=bomr2.apply_no
           WHERE bomr.handle_time < bomr2.handle_time
)omruiacminh ON bao.apply_no =omruiacminh.apply_no

LEFT JOIN (SELECT bomr.apply_no,bomr.handle_time
            FROM(
                SELECT * FROM temp_order_matter_record WHERE matter_key='UploadImg' AND rank3=1 )bomr
            LEFT JOIN (
                SELECT * FROM temp_order_matter_record WHERE  matter_key='ApplyCheck' AND rank1=1
                )bomr2 ON bomr.apply_no=bomr2.apply_no
                WHERE bomr.handle_time < bomr2.handle_time
	)omruiacmaxh ON bao.apply_no =omruiacmaxh.apply_no

LEFT JOIN (SELECT * FROM ods_bpms_biz_isr_mixed_common WHERE is_partner_ding_msg_sent = '1')bimipdms ON bao.apply_no=bimipdms.apply_no
LEFT JOIN (SELECT * FROM temp_order_matter_record_bim WHERE LOWER(matter_key) = 'uploadimg' AND rank =1)omrbim ON bao.apply_no = omrbim.apply_no
LEFT JOIN (SELECT * FROM temp_missing_materials WHERE node_id = "Investigate" AND rankmin =1)tmmmin ON bao.apply_no = tmmmin.apply_no
LEFT JOIN (SELECT * FROM temp_missing_materials WHERE node_id = "Investigate" AND rankmax =1)tmmmax ON bao.apply_no = tmmmax.apply_no
LEFT JOIN (SELECT MIN(a.create_time)AS min_create_time,a.apply_no FROM (SELECT * FROM ods_bpms_biz_p2p_ret WHERE task_name = 'TEAM_ORG_APPROVE') a GROUP BY a.apply_no)bpr ON bao.apply_no = bpr.apply_no
LEFT JOIN (SELECT MIN(a.update_time)AS min_update_time,a.apply_no FROM (SELECT * FROM ods_bpms_biz_p2p_ret WHERE task_name = 'ZG_APPROVE' AND `status` = 1) a GROUP BY a.apply_no )bprza ON bao.apply_no = bprza.apply_no
LEFT JOIN (SELECT MIN(a.update_time)AS min_update_time,a.apply_no FROM (SELECT * FROM ods_bpms_biz_p2p_ret WHERE task_name = 'TEAM_ORG_APPROVE' AND result LIKE "%通过%") a GROUP BY a.apply_no)bprtg ON bao.apply_no = bprtg.apply_no
LEFT JOIN (SELECT MIN(policy_start) AS min_policy_start,apply_no FROM ods_bpms_biz_insurance_policy GROUP BY apply_no)bip ON bip.apply_no = bao.apply_no
LEFT JOIN (SELECT MIN(a.create_time)AS min_create_time,a.apply_no FROM (SELECT * FROM ods_bpms_biz_p2p_ret WHERE task_name = 'LOAN')a GROUP BY a.apply_no)bprl ON bao.apply_no = bprl.apply_no
LEFT JOIN (SELECT MIN(a.update_time)AS min_update_time,a.apply_no FROM (SELECT * FROM ods_bpms_biz_p2p_ret WHERE task_name = 'LOAN' AND result LIKE "%成功%")a GROUP BY a.apply_no)bprls ON bao.apply_no = bprls.apply_no
LEFT JOIN (SELECT MAX(con_funds_time)AS max_con_funds_time,MIN(con_funds_time)AS min_con_funds_time,apply_no FROM ods_bpms_c_fund_module_common GROUP BY apply_no)cfm ON bao.apply_no = cfm.apply_no
LEFT JOIN (
    SELECT MIN(to_company_account_time)AS min_to_company_account_time,MAX(funded_time)AS max_funded_time,MAX(has_funded_repayment)AS has_funded_repayment,MAX(to_company_account_time)AS max_to_company_account_time
	,MIN(return_funded_time)AS min_return_funded_time,apply_no FROM ods_bpms_biz_fund_flow_info GROUP BY apply_no
	)bffi ON bao.apply_no = bffi.apply_no
LEFT JOIN temp_fd_advance_sort tfas ON bao.apply_no=tfas.apply_no
LEFT JOIN (SELECT * FROM temp_fd_advance_ret WHERE adv_type = 'floorAdvance' AND rank =1)farfa ON bao.apply_no=farfa.apply_no
LEFT JOIN (SELECT * FROM temp_fd_advance_ret WHERE adv_type = 'expireAdvance' AND rank =1)farea ON bao.apply_no=farea.apply_no
LEFT JOIN (SELECT MAX(cct.trans_day)AS max_trans_day,cct.apply_no FROM (SELECT * FROM ods_bpms_c_cost_trade WHERE trans_type = 'CC02')cct GROUP BY cct.apply_no )cct ON bao.apply_no = cct.apply_no
LEFT JOIN (SELECT COUNT(1) AS num,a.PROC_INST_ID_ FROM (SELECT * FROM ods_bpms_bpm_check_opinion_common WHERE opinion_ LIKE '%合作机构%')a GROUP BY a.PROC_INST_ID_ )bco ON bao.flow_instance_id=bco.PROC_INST_ID_
LEFT JOIN (SELECT MIN(receive_policy_time)AS min_receive_policy_time,apply_no FROM ods_bpms_biz_insurance_info GROUP BY apply_no)bii ON bao.apply_no=bii.apply_no
LEFT JOIN ods_bpms_bpm_check_opinion_common_ex_wx bco_ex_wx ON bco_ex_wx.apply_no=bao.apply_no
LEFT JOIN (select biz_seq_no,min(UPDATE_TIME) as min_update_time from (select * from ods_msg_if_msg_detail where PROC_STATE = '04' and CONTENT like "%待进行录入完成确认%" )a group by biz_seq_no )imd on bao.apply_no = imd.BIZ_SEQ_NO
LEFT JOIN (SELECT SUP_INST_ID_,min(COMPLETE_TIME_)as complete_time_ FROM ods_bpms_bpm_check_opinion  WHERE  TASK_KEY_ = 'UserTask1'and TASK_NAME_ = "资金归还申请" group by SUP_INST_ID_)zjghsq on zjghsq.SUP_INST_ID_ = bao.flow_instance_id
LEFT JOIN (SELECT SUP_INST_ID_,min(COMPLETE_TIME_)as complete_time_ FROM ods_bpms_bpm_check_opinion  WHERE  TASK_KEY_ = 'UserTask2'and TASK_NAME_ = "资金归还确认" group by SUP_INST_ID_)zjghqr on zjghqr.SUP_INST_ID_ = bao.flow_instance_id
)

INSERT overwrite TABLE dwd.`tmpfact_order_shixiao`
SELECT
cast(dd.id AS bigint) date_key,
cast(nvl(dp.id, 0) AS bigint) product_key,
cast(nvl(nvl(do.s_key, do2.s_key), 0) AS bigint) org_key,
ios.apply_no,
ios.order_create_date,
ios.product_name,
ios.`version`,
ios.shenqing_amount,
ios.mqbc_time,
CAST(ios.mianqian_date AS TIMESTAMP) mianqian_date,
ios.tjlr_date,
CAST(ios.shencha_date AS TIMESTAMP) shencha_date,
ios.bxqq_date,
CAST(ios.fkzlts_date AS TIMESTAMP) fkzlts_date,
ios.fsfkzl_date,
ios.jkqx_date,
ios.daozhang_date,
CAST(ios.dycj_date AS TIMESTAMP) dycj_date,
ios.dgszs_date,
ios.jiebao_date,
ios.ccth_date,
CAST(ios.baoshen_date AS TIMESTAMP) baoshen_date,
ios.bjtz_date,
ios.djwcan_date,
ios.jgfhspjg_date,
ios.hzjgfhfk_date,
CAST(CURRENT_TIMESTAMP() AS TIMESTAMP) etl_update_time,
ios.sales_user_name,
ios.product_id,
ios.mianqian_user,
CAST(ios.sclrldsd_date AS TIMESTAMP) sclrldsd_date,
ios.bj_reason,
ios.bujian_list,
ios.ccbjwc_date,
ios.zzbjwc_date,
CAST(ios.lrtj_date AS TIMESTAMP) lrtj_date,
ios.lrjbr_name,
ios.cfdeclr_date,
ios.fkclwt_date,
ios.tzlrwc_date,
CAST(ios.lrld_date AS TIMESTAMP) lrld_date,
CAST(ios.lrwc_date AS TIMESTAMP) lrwc_date,
ios.lrwc_name,
ios.baoshen_user,
ios.shencha_result,
ios.shencha_opinion,
ios.shencha_user,
CAST(ios.scsd_date AS TIMESTAMP) scsd_date,
ios.sp_zlbc_date,
CAST(ios.shenpi_date AS TIMESTAMP) shenpi_date,
ios.shenpi_result,
ios.shenpi_opinion,
ios.shenpi_user,
ios.fjsp_flag,
CAST(ios.wczlts_date AS TIMESTAMP) wczlts_date,
ios.wczlts_user,
CAST(ios.spzlwc_date AS TIMESTAMP) `spzlwc_date`,
ios.spzlwc_user,
CAST(ios.spjgqr_date AS TIMESTAMP) spjgqr_date,
ios.spjgqr_user,
ios.fkzlts_user,
ios.fsfkzl_user,
ios.zjghsq_date,
ios.zjghsq_use,
ios.zjghqr_date,
ios.slq_dzsq_date,
ios.slq_dz_date,
ios.slq_gh_date,
ios.slq_dz_reason,
ios.dqgh_dzsq_date,
ios.dqgh_dzdz_date,
ios.dqgh_gh_date,
ios.dqgh_dz_reason,
ios.is_change_plat,
ios.zjghqr_user,
CAST(ios.sldj_date AS TIMESTAMP) sldj_date,
CAST(ios.lqzxzl_date AS TIMESTAMP) lqzxzl_date,
CAST(ios.zxdy_date AS TIMESTAMP) zxdy_date,
CAST(ios.ghdj_date AS TIMESTAMP) ghdj_date,
CAST(ios.ghcj_date AS TIMESTAMP) ghcj_date,
CAST(ios.dydj_date AS TIMESTAMP) dydj_date,
ios.yhfk_date ,
ios.hptk_date ,
CAST(ios.gd_date AS TIMESTAMP) gd_date,
CAST(ios.hbzlwc_date AS TIMESTAMP) hbzlwc_date,
ios.cbd_date,
ios.partner_bank_name
FROM `ids_order_shixiao` ios
LEFT JOIN dws.dimension_date dd ON ios.order_create_date_d = dd.calendar
LEFT JOIN dws.dimension_product dp ON ios.product_name = dp.product_name
LEFT JOIN dws.dws_sys_user_auto_fill DO
 	ON ios.sales_user_id = do.user_id AND ios.branch_id = do.sub_company_id
 	AND ios.sales_branch_id = do.sub_department_id
    AND do.job_name LIKE "%渠道经理%"
 	AND do.sub_department_name LIKE "%市场%"

LEFT JOIN dws.dws_sys_user_auto_fill do2
	ON ios.sales_user_id = do2.user_id AND ios.branch_id = do2.sub_company_id
 	AND ios.sales_branch_id = do.sub_department_id
    AND do2.job_name = "市场主管岗"
;
DROP TABLE IF EXISTS dwd.dwd_fact_order_shixiao;
ALTER TABLE dwd.tmpfact_order_shixiao RENAME TO dwd.dwd_fact_order_shixiao;