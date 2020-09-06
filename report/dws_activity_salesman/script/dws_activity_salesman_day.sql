drop table if exists dws.dws_activity_salesman_day;
CREATE TABLE dws.dws_activity_salesman_day (
  `report_date` timestamp COMMENT '日期',
  `city` string COMMENT '公司',
  `market_department` string COMMENT '市场部',
  `position` string COMMENT '职位',
  `name` string COMMENT '销售人员',
  `has_reach_goal` string COMMENT '是否达标',
  `activity_num` double COMMENT '折算活动量',
  `interview_sign_num` double COMMENT '面签笔数',
  `loan_num` double COMMENT '放款笔数',
  `visit_num` double COMMENT '拜访次数',
  `csh_num` double COMMENT '产说会次数',
  `staff_no` string COMMENT '工号',
  `higher_level_no` string COMMENT '上级工号',
  `higher_level_name` string COMMENT '上级姓名',
  `user_type` string COMMENT '用户类型（大道：DADAO,加盟商：FRANC）',
  `etl_update_time` timestamp COMMENT '更新时间'
);
------------------------------------------
--日活动量统计-销售人员
------------------------------------------
insert overwrite table dws.dws_activity_salesman_day
select 
vv.report_date --日期
,vv.city --公司
,vv.market_department --市场部
,vv.position --'职位',
,vv.`name` --销售人员
,if((vv.visit_num + (vv.interview_sign_num + vv.loan_num + vv.csh_num) *4) >= 6,'是','否') has_reach_goal --是否达标
,(vv.visit_num + (vv.interview_sign_num + vv.loan_num + vv.csh_num) * 4) activity_num --折算活动量
,vv.interview_sign_num --面签笔数
,vv.loan_num --放款笔数
,vv.visit_num --拜访次数
,vv.csh_num --产说会次数
,vv.staff_no --工号
,vv.higher_level_no --上级工号
,vv.higher_level_name --上级姓名
,vv.user_type --用户类型（大道：DADAO,加盟商：FRANC）
,from_unixtime(unix_timestamp(),'yyyy-MM-dd')
from dws.tmp_report_activity_base vv
where vv.position = "销售人员"
and vv.report_date > '2018-12-31' and vv.report_date < from_unixtime(unix_timestamp(),'yyyy-MM-dd') and vv.isworkday=1