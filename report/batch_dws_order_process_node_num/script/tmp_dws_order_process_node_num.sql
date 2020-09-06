-- create portion
set hive.execution.engine=spark;
use dws;
drop table if exists dws.tmp_dws_order_process_node_num;
create table if not exists dws.tmp_dws_order_process_node_num (
    apply_no string COMMENT '订单号' ,
	branch_name_1_level string COMMENT '分公司_一级',
	org_name string COMMENT '市场部门',
	org_leader string COMMENT '团队长',
	sales_user_name string COMMENT '渠道经理',
	product_name string COMMENT '产品名称',
	interview_save_time string COMMENT '面签保存时间',
	interview_time string COMMENT '面签时间',
	max_approval_time string COMMENT '最终审批时间',
	max_approval_status string COMMENT '最终审批结果',
	loan_time_xg string COMMENT '放款时间_销管',
	financial_archive_event_time string COMMENT '财务归档时间',
	apply_status_name string COMMENT '订单状态',
	loan_num_xg string COMMENT '放款笔数_销管',
	interview_num_xg string COMMENT '面签笔数_销管',
	release_amount_xg string COMMENT '放款金额',
  max_payment_time string COMMENT '回款时间',
  product_type string COMMENT '产品类型',
  etl_update_time string COMMENT 'etl时间'

) stored as parquet;

-- with portion
with tmp_org as (
select
org_code,
org_name_, -- 所属团队
concat_ws(',',collect_set(case when user_post like "%市场%主管%" then fullname_ end)) org_leader -- 团队长_市场主管
from ods.ods_bpms_sys_org_user_common  where org_name_ like '%市场%'
group by org_code,org_name_
),

tmp_opinion_desc as(
select 
tmp.PROC_INST_ID_
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_STATUS_ 
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_STATUS_ 
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_STATUS_ 
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_STATUS_
,min(case when tmp.TASK_KEY_ = 'Interview' then tmp.COMPLETE_TIME_ end ) interview_time -- 面签时间
,min(case when tmp.TASK_KEY_ = 'UserTask2' and TASK_NAME_ = "面签" then tmp.COMPLETE_TIME_ end ) interview_time_v1 -- 面签时间
,min(case when tmp.TASK_KEY_ = 'DownHouseSurvey' then tmp.COMPLETE_TIME_ end ) DownHouseSurvey_time -- 下户时间
,min(case when tmp.TASK_KEY_ = 'DownHouseSurvey' then tmp.AUDITOR_NAME_ end ) DownHouseSurvey_user -- 下户经办人
,min(case when tmp.TASK_KEY_ = 'UserTask5' then tmp.COMPLETE_TIME_ end ) send_loan_command_time_v1 -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'SendLoanCommand' then tmp.COMPLETE_TIME_ end ) send_loan_command_time -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'AgreeLoanMark' then tmp.COMPLETE_TIME_ end ) agreeloanmark_time  --  同贷信息登记时间
,min(case when tmp.TASK_KEY_ = 'CostConfirm' then tmp.COMPLETE_TIME_ end ) costconfirm_time -- 缴费确认时间
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) investigate_time_tfb --审查时间  郑州提放保  "NSL-TFB371", "SL-TFB371"
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) investigate_time_v2 --审查时间
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) investigate_time_v25 -- 审查时间
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) investigate_time_v1 -- 审查时间
,min(case when tmp.TASK_KEY_ = 'RandomMark' then tmp.COMPLETE_TIME_ end ) random_mark_time -- 赎楼登记时间
,min(case when tmp.TASK_KEY_ = 'CancleMortgage' then tmp.COMPLETE_TIME_ end )  cancle_mortage_time -- 注销抵押时间
,min(case when tmp.TASK_KEY_ = 'TransferIn' then tmp.COMPLETE_TIME_ end ) transfer_in_time  -- 过户递件时间 
,min(case when tmp.TASK_KEY_ = 'TransferOut' then tmp.COMPLETE_TIME_ end ) transfer_out_time -- 过户出件
,min(case when tmp.TASK_KEY_ = 'MortgagePass' then tmp.COMPLETE_TIME_ end ) mortgage_pass_time -- 抵押递件时间
,min(case when tmp.TASK_KEY_ = 'MortgageOut' then tmp.COMPLETE_TIME_ end ) mortgage_out_time -- 抵押出件时间
,min(case when tmp.TASK_KEY_ = 'OverInsurance' then tmp.COMPLETE_TIME_ end ) over_insurance_time -- 解保时间
,min(case when tmp.TASK_KEY_ = 'financialArchiveEvent' then tmp.COMPLETE_TIME_ end ) financial_archive_event_time -- 财务归档时间

from (
  select 
    b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_ desc ) rank
    from ods.ods_bpms_bpm_check_opinion_common_ex b 
) tmp

where rank = 1
group by tmp.PROC_INST_ID_
),

tmp_bco as (
  select apply_no,
  concat_ws(',',collect_set(OPINION_))OPINION_
  from ods.ods_bpms_bpm_check_opinion_common_ex 
  where `STATUS_`='manual_end' 
  group by apply_no
  ),

tmp_trade_cc01 as (
  select obcct.apply_no, max(obcct.trans_day) max_cc01_time, sum(obcct.trans_money) total_cc01_money 
  from ods.ods_bpms_c_cost_trade obcct
  where obcct.trans_type = "CC01" 
  group by obcct.apply_no
),

tmp_obbh as (
  select bh.house_no, count(*) fcsl 
  from ods.ods_bpms_biz_house bh 
  where bh.house_type not in ("qt", "cw") and bh.house_no is not null and bh.house_no <> ""
  group by bh.house_no 
),

tmp_brf as (
  select * from ods.ods_bpms_biz_ransom_floor_common where rn=1
)


-- insert portion
insert overwrite table dws.tmp_dws_order_process_node_num
SELECT
  bao.apply_no -- 订单号
, dc.company_name_2_level  branch_name_1_level  -- 1级分公司
, sys.org_name_ org_name -- 市场部门
, sys.org_leader -- 团队长
, bao.sales_user_name -- 渠道经理
, bao.product_name -- 产品名称
, bim.interview_save_time -- 面签保存时间
, t666.interview_time_min interview_time -- 面签时间
, nvl(
  case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.investigate_time_tfb   -- 郑州提放保
         when bao.product_version in ('v1.5', 'v1.0') then opid.investigate_time_tfb
         when bao.product_version = 'v2.0' then opid.investigate_time_v2
         when bao.product_version = 'v2.5' then nvl(opid.investigate_time_v2, opid.investigate_time_v25)
  end -- 审批时间 
  ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.investigate_time_tfb   -- 郑州提放保
         when bao.product_version in ('v1.5', 'v1.0') then opid.investigate_time_v1
         when bao.product_version = 'v2.0' then opid.investigate_time_v2
         when bao.product_version = 'v2.5' then opid.investigate_time_v25
   end  -- 审查时间
) max_approval_time -- 最终审批时间
, nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
       when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_STATUS_
       when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
       when bao.product_version = 'v2.5' then nvl(opid.ManCheck_STATUS_, opid.Investigate_STATUS_)
    end  -- 审批结果
  ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
       when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_STATUS_
       when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
       when bao.product_version = 'v2.5' then opid.Investigate_STATUS_
   end  -- 审查结果
) max_approval_status -- 最终审批结果
,oofc.loan_time_xg -- 放款时间_销管
,opid.financial_archive_event_time -- 财务归档时间
,bao.apply_status_name -- 订单状态
,(case when oofc.loan_time_xg is null then 0
    else
      (case
            when bao.product_name='保险服务'
               or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
               or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
               then 0
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
            when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl=0)
               then 1  -- 和放款笔数折算不同
            when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
               then 0.5
            else 0
         end )
   end )  loan_num_xg -- 放款笔数销管
,(case when t666.interview_time_min is null  then 0
    else
     (case
        when bao.product_name='保险服务'
           or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
           or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
           then 0
        when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
        when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and nvl(obbh.fcsl, 0)=0)
           then 1  -- 和放款笔数折算不同
        when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
           then 0.5
        else 0
     end )
end ) interview_num_xg  -- 面签笔数 销管口径
,(case 
   when bao.product_type='现金类产品' then cfm.con_funds_cost
   when bao.product_name like'买付保%' then bfs.guarantee_amount    
   else nvl(bnl.biz_loan_amount, 0)
 end) release_amount_xg  -- 放款金额（名字加了销管而已，并不是销管的计算方式）
,trade_cc01.max_cc01_time max_payment_time  -- 最后一次回款时间 
,bao.product_type -- 产品类型
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from ods.ods_bpms_biz_apply_order_common bao
left join dim.dim_sys_org dso on dso.branch_id = bao.branch_id
left join tmp_org sys on sys.org_code=bao.sales_branch_id
left join ods.ods_bpms_biz_isr_mixed_common bim on bim.apply_no=bao.apply_no
left join ods.ods_bpms_biz_fee_summary bfs on bfs.apply_no=bao.apply_no
left join ods.ods_bpms_sys_dic sd_pt on bfs.`price_tag`=sd_pt.KEY_ and sd_pt.`TYPE_ID_`='10000042640043'
left join tmp_brf brf on brf.apply_no=bao.apply_no
left join ods.ods_bpms_c_fund_module_common cfm on cfm.apply_no=bao.apply_no
left join tmp_obbh obbh on obbh.house_no = bao.house_no
LEFT JOIN (select * from ods.ods_bpms_biz_new_loan_common where rn = 1) bnl on bnl.apply_no=bao.apply_no
left join tmp_trade_cc01 trade_cc01 on trade_cc01.apply_no = bao.apply_no
left join tmp_opinion_desc opid on opid.PROC_INST_ID_ = bao.flow_instance_id 
left join tmp_bco bco on bco.apply_no=bao.apply_no
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join ods.ods_orders_finance_common oofc on oofc.apply_no = bao.apply_no
left join dim.dim_company dc on bao.branch_id = dc.company_id_3_level