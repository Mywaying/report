set hive.execution.engine=spark;
drop table if exists dws.dws_activity_salesman_month;
CREATE TABLE dws.dws_activity_salesman_month (
  `report_month` string COMMENT '年月',
  `city` string COMMENT '分公司',
  `market_department` string COMMENT '市场部',
  `name` string COMMENT '销售人员',
  `has_reach_goal` string COMMENT '是否达标',
  `reach_goal_day_rate` double COMMENT '达标天数比例',
  `total_activity_num` double COMMENT '活动量合计',
  `day_avg_activity_num` double COMMENT '日均活动量',
  `reach_goal_day` double COMMENT '累计达标天数',
  `csh_num` double COMMENT '累计产说会次数',
  `interview_sign_num` double COMMENT '面签笔数',
  `loan_num` double COMMENT '放款笔数',
  `visit_num` double COMMENT '拜访次数',
  `reach_goal_total_rate` double COMMENT '累计活动量达标率',
  `isworkday` bigint comment '工作日天数',
  `staff_no` string COMMENT '工号',
  `higher_level_no` string COMMENT '上级工号',
  `higher_level_name` string COMMENT '上级姓名',
  `user_type` string COMMENT '用户类型（大道：DADAO,加盟商：FRANC）',
  `etl_update_time` timestamp COMMENT '更新时间'
);
------------------------------------------------------------------------------
----月活动量统计-销售人员  
------------------------------------------------------------------------------
insert overwrite table dws.dws_activity_salesman_month
select 
substr(cast(mm.report_date as string), 1, 7) report_month,--年月
mm.city,--分公司
mm.market_department,--市场部
mm.`name`,--销售人员
if(sum(mm.has_reach_goal)/sum(mm.isworkday)>0.8 or sum(mm.activity_num)/(sum(mm.isworkday)*6)>=1,'是','否') has_reach_goal,--'是否达标',
sum(mm.has_reach_goal)/sum(mm.isworkday) reach_goal_day_rate,--达标天数比例
sum(mm.activity_num) total_activity_num,--活动量合计
sum(mm.activity_num)/sum(mm.isworkday) day_avg_activity_num,--日均活动量
sum(mm.has_reach_goal) reach_goal_day, --累计达标天数
sum(mm.csh_num) csh_num, --累计产说会次数',
sum(mm.interview_sign_num) interview_sign_num, --面签笔数
sum(mm.loan_num) loan_num, --放款笔数
sum(mm.visit_num) visit_num, --拜访次数
sum(mm.activity_num)/(sum(mm.isworkday)*6) reach_goal_total_rate, --累计活动量达标率
sum(mm.isworkday) isworkday, --工作日天数
staff_no,--工号
higher_level_no, --上级工号
higher_level_name, --上级姓名
user_type, --用户类型（大道：DADAO,加盟商：FRANC）
from_unixtime(unix_timestamp(),'yyyy-MM-dd')
from
(   
  select 
  vv.*,
  if((vv.visit_num+(vv.interview_sign_num+vv.loan_num+vv.csh_num)*4)>=6,'1','0') has_reach_goal,--是否达标
  (vv.visit_num+(vv.interview_sign_num+vv.loan_num+vv.csh_num)*4) activity_num --折算活动量
  from
  dws.tmp_report_activity_base vv
  where vv.position = "销售人员"
  and vv.report_date > '2018-12-31' and vv.report_date < from_unixtime(unix_timestamp(),'yyyy-MM-dd') and vv.isworkday=1
)mm
group by  mm.city, mm.market_department, substr(cast(mm.report_date as string), 1, 7), mm.`name`, staff_no, higher_level_no, higher_level_name, user_type
