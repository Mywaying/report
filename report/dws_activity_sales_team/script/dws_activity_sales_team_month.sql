set hive.execution.engine=spark;
drop table if exists dws.dws_activity_sales_team_month;
CREATE TABLE dws.dws_activity_sales_team_month (
  `report_month` string COMMENT '年月',
  `city` string COMMENT '分公司',
  `market_department` string COMMENT '市场部',
  `team_leader_no` string COMMENT '团队长工号',
  `team_leader_name` string COMMENT '团队长姓名',
  `has_reach_goal` string COMMENT '是否达标',
  `total_activity_num` double COMMENT '活动量合计',
  `reach_goal_day_rate` double COMMENT '达标天数比例',
  `reach_goal_day` double COMMENT '累计达标天数',
  `avg_activity_num` double COMMENT '日人均活动量',
  `isworkday` bigint comment '工作日天数',
  `person_num` double COMMENT '人数',
  `interview_sign_num` double COMMENT '面签笔数',
  `loan_num` double COMMENT '放款笔数',
  `visit_num` double COMMENT '拜访次数',
  `csh_num` double COMMENT '产说会次数',
  `higher_level_no` string COMMENT '上级工号',
  `higher_level_name` string COMMENT '上级姓名',
  `user_type` string COMMENT '用户类型（大道：DADAO,加盟商：FRANC）',
  `etl_update_time` timestamp COMMENT '更新时间'
);
------------------------------------------------------------------------------
----月活动量统计-销售团队
------------------------------------------------------------------------------
insert overwrite table dws.dws_activity_sales_team_month
select 
substr(cast(nn.report_date as string), 1, 7) report_month,--年月
nn.city,--分公司
nn.market_department, --市场部
nn.team_leader_no, --团队长工号
nn.team_leader_name,--团队长姓名
if(sum(case when nn.has_reach_goal = '是' then 1 else 0 end)/sum(nn.isworkday)>0.8,'是','否') has_reach_goal,--是否达标
sum(nn.total_activity_num) total_activity_num,--活动量合计
sum(case when nn.has_reach_goal = '是' then 1 else 0 end)/sum(nn.isworkday) reach_goal_day_rate,--'达标天数比例
sum(case when nn.has_reach_goal = '是' then 1 else 0 end) reach_goal_day,--累计达标天数
sum(nn.total_activity_num)/sum(nn.isworkday)/avg(person_num) avg_activity_num,--'日人均活动量',
avg(nn.isworkday) isworkday,--工作日天数
avg(person_num) person_num,--人数
sum(nn.interview_sign_num) interview_sign_num,--'面签笔数',
sum(nn.loan_num) loan_num,--'放款笔数',
sum(nn.visit_num) visit_num,--'拜访次数',
sum(nn.csh_num) csh_num,--'产说会次数'
nn.higher_level_no,--上级工号
nn.higher_level_name,--上级姓名
nn.user_type, --用户类型（大道：DADAO,加盟商：FRANC）
from_unixtime(unix_timestamp(),'yyyy-MM-dd')
from dws.dws_activity_sales_team_day nn
group by nn.city,nn.market_department, substr(cast(nn.report_date as string), 1, 7), nn.team_leader_no, nn.team_leader_name, nn.higher_level_no, nn.higher_level_name, nn.user_type

