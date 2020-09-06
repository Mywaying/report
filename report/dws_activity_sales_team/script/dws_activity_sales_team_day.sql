set hive.execution.engine=spark;
drop table if exists dws.dws_activity_sales_team_day;
CREATE TABLE dws.dws_activity_sales_team_day(
  `report_date` timestamp COMMENT '日期',
  `city` string COMMENT '分公司',
  `market_department` string COMMENT '市场部',
  `team_leader_no` string COMMENT '团队长工号',
  `team_leader_name` string COMMENT '团队长姓名',
  `person_num` double COMMENT '人数',
  `reach_goal_person_num` double COMMENT '达标人数',
  `has_reach_goal` string COMMENT '是否达标',
  `reach_goal_person_rate` double COMMENT '人员达标率',
  `reach_goal_total_rate` double COMMENT '累计活动量达标率',
  `total_activity_num` double COMMENT '活动量合计',
  `person_avg_activity_num` double COMMENT '人均活动量',
  `interview_sign_num` double COMMENT '面签笔数',
  `loan_num` double COMMENT '放款笔数',
  `visit_num` double COMMENT '拜访次数',
  `csh_num` double COMMENT '产说会次数',
  `higher_level_no` string COMMENT '上级工号',
  `higher_level_name` string COMMENT '上级姓名',
  `user_type` string COMMENT '用户类型（大道：DADAO,加盟商：FRANC）',
  `isworkday` double,
  `etl_update_time` timestamp COMMENT '更新时间'
);
------------------------------------------------------------------------------
--日活动量统计-销售团队
------------------------------------------------------------------------------
insert overwrite table dws.dws_activity_sales_team_day
select 
  mm.report_date,--日期
  mm.city,--分公司
  mm.market_department,--市场部
  min(mm.higher_level_no) team_leader_no,--团队长工号
  min(mm.higher_level_name) team_leader_name,--团队长姓名
  count(distinct mm.staff_no) person_num,--人数
  sum(mm.has_reach_goal) reach_goal_person_num,--达标人数
  if(sum(mm.has_reach_goal)/count(distinct mm.staff_no)>0.8  
  or sum(mm.activity_num)/(count(distinct mm.staff_no)*6)>=1,
  '是','否') has_reach_goal,--是否达标
  sum(mm.has_reach_goal)/count(distinct mm.staff_no) reach_goal_person_rate,--人员达标率
  sum(mm.activity_num)/(count(distinct mm.staff_no)*6) reach_goal_total_rate,--累计活动量达标率
  sum(mm.activity_num) total_activity_num,--活动量合计
  nvl(sum(mm.activity_num)/count(distinct mm.staff_no), 0) person_avg_activity_num,--人均活动量
  sum(mm.interview_sign_num) interview_sign_num,--'面签笔数',
  sum(mm.loan_num) loan_num,--'放款笔数',
  sum(mm.visit_num) visit_num,--'拜访次数',
  sum(mm.csh_num) csh_num,--'产说会次数'
  min(bui.higher_level_no),--上级工号
  min(bui.higher_level_name),--上级姓名
  mm.user_type , --用户类型（大道：DADAO,加盟商：FRANC）
  avg(mm.isworkday) isworkday,
  from_unixtime(unix_timestamp(),'yyyy-MM-dd')
  from
  (------活动人员统计
    select vv.*,
    if((vv.visit_num+(vv.interview_sign_num+vv.loan_num+vv.csh_num)*4)>=6,1,0) has_reach_goal,--是否达标
    (vv.visit_num+(vv.interview_sign_num+vv.loan_num+vv.csh_num)*4) activity_num--折算活动量
    from
    dws.tmp_report_activity_base vv
    where vv.position  = "销售人员" and vv.report_date>'2018-12-31' and vv.report_date < from_unixtime(unix_timestamp(),'yyyy-MM-dd') and vv.isworkday=1
  )mm
left join ods.ods_spp_biz_user_info bui on bui.staff_no=mm.higher_level_no
group by mm.city, mm.market_department, mm.report_date, mm.user_type