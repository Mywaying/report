-- set logdate='2019-01-01';

-- drop table if exists dws.dws_order_process_node_num;
-- create table if not exists dws.dws_order_process_node_num (
--     report_date string  COMMENT '日期',
-- 	branch_name string  COMMENT '分公司_一级',
-- 	org_name string  COMMENT '市场部',
-- 	org_leader string  COMMENT '团队长',
-- 	sales_user_name string  COMMENT '渠道经理',
-- 	product_name string  COMMENT '产品名称',
-- 	interview_save_num bigint  COMMENT '面签保存订单量',
-- 	interview_num bigint  COMMENT '面签订单量',
-- 	approval_num bigint  COMMENT '审批通过订单量',
-- 	loan_num bigint  COMMENT '放款订单量',
-- 	release_amount string  COMMENT '放款金额',
-- 	fin_archive_num bigint  COMMENT '财务归档订单量',
-- 	end_not_archive_num bigint  COMMENT '已完结未财务归档订单量',
-- 	etl_update_time string COMMENT 'etl时间'
-- ) stored as parquet;

-- with portion
set hive.execution.engine=spark;
use dws;
with tmp_interview_save as (
	select 
	to_date(interview_save_time)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) interview_save_num -- 面签保存订单量
	from dws.tmp_dws_order_process_node_num 
	where to_date(interview_save_time) = '${hivevar:logdate}'
	group by to_date(interview_save_time), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_interview as (
	select 
	to_date(interview_time)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) interview_num -- 面签订单量
	from dws.tmp_dws_order_process_node_num 
	where to_date(interview_time) = '${hivevar:logdate}'
	group by to_date(interview_time), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_approval as (
	select 
	to_date(max_approval_time)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) approval_num -- 审批通过订单量
	from dws.tmp_dws_order_process_node_num 
	where to_date(max_approval_time) = '${hivevar:logdate}'and max_approval_status = "agree"
	group by to_date(max_approval_time), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_loan as (
	select 
	to_date(loan_time_xg)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) loan_num -- 放款订单量
	,sum(cast(release_amount_xg as bigint)) release_amount -- 放款金额
	from dws.tmp_dws_order_process_node_num 
	where to_date(loan_time_xg) = '${hivevar:logdate}'
	group by to_date(loan_time_xg), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_financial_archive as (
	select 
	to_date(financial_archive_event_time)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) fin_archive_num -- 财务归档订单量
	from dws.tmp_dws_order_process_node_num 
	where to_date(financial_archive_event_time) = '${hivevar:logdate}'
	group by to_date(financial_archive_event_time), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_csh_end_not_archive as (
	select 
	to_date(max_payment_time)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) csh_end_not_archive_num -- 现金类产品 已完结未财务归档订单量
	from dws.tmp_dws_order_process_node_num 
	where financial_archive_event_time is null and to_date(max_payment_time) = '${hivevar:logdate}'and product_type = "现金类产品"
	group by to_date(max_payment_time), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
),

tmp_notcsh_end_not_archive as (
	select 
	to_date(loan_time_xg)  report_date 
	, branch_name_1_level branch_name
	, org_name
	, org_leader 
	, sales_user_name
	, product_name
	,count(apply_no) notcsh_end_not_archive_num -- 现金类产品 已完结未财务归档订单量
	from dws.tmp_dws_order_process_node_num 
	where financial_archive_event_time is null and to_date(loan_time_xg) = '${hivevar:logdate}'and product_type <> "现金类产品"
	group by to_date(loan_time_xg), branch_name_1_level, org_name, org_leader, sales_user_name, product_name
)

insert into table dws.dws_order_process_node_num

select 
dd.id date_key 
, branch_name
, org_name
, org_leader 
, sales_user_name
, dp.id product_key
, interview_save_num
, interview_num
, approval_num
, loan_num
, release_amount
, fin_archive_num
, end_not_archive_num
, from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from 
(
select 
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, sum(interview_save_num) interview_save_num
, sum(interview_num) interview_num
, sum(approval_num) approval_num
, sum(loan_num) loan_num
, sum(release_amount) release_amount
, sum(fin_archive_num) fin_archive_num
, sum(csh_end_not_archive_num) + sum(notcsh_end_not_archive_num)  end_not_archive_num
from 
(
SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num-- 已完结未财务归档订单量-现金
, 0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_interview_save

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num-- 已完结未财务归档订单量-现金
, 0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_interview

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num-- 已完结未财务归档订单量-现金
, 0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_approval

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, loan_num -- 放款订单量
, release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num-- 已完结未财务归档订单量-现金
, 0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_loan

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num -- 已完结未财务归档订单量-现金
, 0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_financial_archive

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, csh_end_not_archive_num  -- 已完结未财务归档订单量-现金
,0 notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_csh_end_not_archive  a

union all 

SELECT
report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
, 0 interview_save_num -- 面签保存订单量
, 0 interview_num -- 面签订单量
, 0 approval_num -- 审批通过订单量
, 0 loan_num -- 放款订单量
, 0 release_amount -- 放款金额
, 0 fin_archive_num -- 财务归档订单量
, 0 csh_end_not_archive_num  -- 已完结未财务归档订单量-现金
, notcsh_end_not_archive_num -- 已完结未财务归档订单量-非现金
from tmp_notcsh_end_not_archive  a
) as a

group by report_date 
, branch_name
, org_name
, org_leader 
, sales_user_name
, product_name
)as a 
left join dws.dimension_product dp on a.product_name = dp.product_name
left join dws.dimension_date dd on a.report_date = dd.calendar