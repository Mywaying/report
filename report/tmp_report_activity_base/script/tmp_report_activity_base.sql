-- init sql------------------------------------------------
set hive.execution.engine=spark;
drop table if exists dws.tmp_report_activity_base;
CREATE TABLE dws.tmp_report_activity_base (
  `report_date` timestamp,
  `isworkday` bigint,
  `id` string,
  `staff_no` string,
  `name` string,
  `mobile_no` string,
  `position` string,
  `city` string,
  `market_department` string,
  `user_type` string,
  `higher_level_no` string,
  `higher_level_name` string,
  `create_user_id` string,
  `create_date` timestamp,
  `update_user_id` string,
  `update_date` timestamp ,
  `interview_sign_num` double ,
  `loan_num` double ,
  `visit_num` double ,
  `csh_num` double,
  `etl_update_time` timestamp COMMENT '更新时间'
);


with j as(
-- 面签
	select 
	to_date(x.interview_time) report_date
	,su.mobile_ sales_user_phone
	,sum(x.interview_num_xg) interview_sign_num
	from dwd.dwd_orders_ex x 
 	join ods.ods_bpms_biz_apply_order bao on x.apply_no = bao.apply_no
 	join ods.ods_bpms_sys_user su on bao.sales_user_id = su.id_
	group by to_date(x.interview_time), su.mobile_
),

h as(
-- 活动入口
	select
	to_date(d.create_time) report_date
	,d.phone
	,count(case when d.template_name='产说会/同业交流会' then d.dingtalk_id end) csh_num
	,count(case when d.template_name in ('渠道拜访记录', "拜访记录") then d.dingtalk_id end) visit_num
	from ods.ods_ding_ding_log d
	where d.template_name in ('产说会/同业交流会','渠道拜访记录', "拜访记录") 
	group by to_date(d.create_time), d.phone
),

k as(
-- 放款
	select 
	to_date(y.loan_time_xg) report_date
	,su.mobile_ sales_user_phone
	,sum(y.loan_num_xg) loan_num
	from dwd.dwd_orders_ex y
	join ods.ods_bpms_biz_apply_order bao on y.apply_no = bao.apply_no
 	join ods.ods_bpms_sys_user su on bao.sales_user_id = su.id_
	group by to_date(y.loan_time_xg), su.mobile_ 
) 

insert overwrite table dws.tmp_report_activity_base
select
p.`report_date`
,p.`is_work_day` 
,cast(p.`id` as string)
,p.`staff_no` 
,p.`name` 
,p.`mobile_no`
,p.`position` 
,p.`branch_company` 
,p.`market_department`
,p.`user_type` 
,p.`higher_level_no` 
,p.`higher_level_name` 
,p.`create_user_id` 
,p.`create_date` 
,p.`update_user_id` 
,p.`update_date` 
,nvl(j.interview_sign_num, 0) interview_timesign_num
,nvl(k.loan_num, 0) loan_num
,nvl(h.visit_num, 0) visit_num
,nvl(h.csh_num, 0) csh_num
,from_unixtime(unix_timestamp(),'yyyy-MM-dd')
from(
	-- 日期+人员辅助计算
	select to_date(r.day) report_date, r.is_work_day,u.*
	from ods.ods_spp_biz_user_info u, dim.dim_c_work_calendar r  
	where to_date(r.day)  >= '2018-12-31' 
	and to_date(r.day) < from_unixtime(unix_timestamp(),'yyyy-MM-dd')
) p
left join h on p.mobile_no=h.phone and p.report_date=h.report_date
left join j on p.mobile_no=j.sales_user_phone and p.report_date=j.report_date
left join k on p.mobile_no=k.sales_user_phone  and p.report_date=k.report_date;