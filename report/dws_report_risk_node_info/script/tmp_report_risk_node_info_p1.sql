set hive.execution.engine=spark;
drop table if exists dwd.tmp_report_risk_node_info_p1;
create table if not exists dwd.tmp_report_risk_node_info_p1 (
  `apply_no` string ,
  `PROC_DEF_KEY_` string ,
  `product_version` string  COMMENT '版本',
  `flow_instance_id` string ,
  `f_sc_date` timestamp  COMMENT '第一次审查时间',
  `f_sc_status` string  COMMENT '第一次审查结果',
  `f_sc_opinion` string COMMENT '第一次审查建议',
  `f_sc_user_name` string  COMMENT '第一次审查人员',
  `s_sc_date` timestamp  COMMENT '第二次审查时间'
);

with tmp_check_opinion as (

    select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.AUDITOR_NAME_ end ) UserTask3_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) UserTask3_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.OPINION_ end ) UserTask3_opinion
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.AUDITOR_NAME_ end ) UserTask4_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) UserTask4_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.OPINION_ end ) UserTask4_opinion
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.AUDITOR_NAME_ end ) ManCheck_user
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) ManCheck_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_status
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.OPINION_ end ) ManCheck_opinion
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.AUDITOR_NAME_ end ) Investigate_user
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) Investigate_time
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_status
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.OPINION_ end ) Investigate_opinion
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.COMPLETE_TIME_ is not null
    ) tmp
    where rank = 1
    group by tmp.PROC_INST_ID_
),

tmp_check_opinion_rank_2 as (

    select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) UserTask4_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) ManCheck_time
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) Investigate_time
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.COMPLETE_TIME_ is not null and b.TASK_KEY_ in ('UserTask4', 'ManCheck', 'Investigate')
    ) tmp
    where rank = 2
    group by tmp.PROC_INST_ID_
)

insert overwrite table dwd.tmp_report_risk_node_info_p1
select
t1.apply_no,
t2.PROC_DEF_KEY_ ,
t1.product_version,
t1.flow_instance_id,
tco.UserTask3_time f_sc_date, --  第一次审查时间
tco.UserTask3_status f_sc_status,
tco.UserTask3_opinion f_sc_opinion,
tco.UserTask3_user  f_sc_user_name,
tco_r2.UserTask4_time s_sc_date --  第二次审查时间
from ods.ods_bpms_biz_apply_order_common t1
left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
left join tmp_check_opinion_rank_2 tco_r2 on t1.flow_instance_id = tco_r2.PROC_INST_ID_
WHERE t2.PROC_DEF_KEY_ IN (
  'bizApply_all_cash',
  'bizApply_all_insure'
)

union all

select
t1.apply_no,
t2.PROC_DEF_KEY_ ,
t1.product_version,
t1.flow_instance_id,
tco.UserTask4_time f_sc_date,
tco.UserTask4_status f_sc_status,
tco.UserTask4_opinion f_sc_opinion,
tco.UserTask4_user f_sc_user_name,
tco_r2.UserTask4_time s_sc_date
from ods.ods_bpms_biz_apply_order_common t1
left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
left join tmp_check_opinion_rank_2 tco_r2 on t1.flow_instance_id = tco_r2.PROC_INST_ID_
WHERE t2.PROC_DEF_KEY_ IN (
  'bizApply_zztfb'
)

union all

select
t1.apply_no,
t2.PROC_DEF_KEY_ ,
t1.product_version,
t1.flow_instance_id,
tco.ManCheck_time f_sc_date,
tco.ManCheck_status f_sc_status,
tco.ManCheck_opinion f_sc_opinion,
tco.ManCheck_user f_sc_user_name,
tco_r2.ManCheck_time s_sc_date
from ods.ods_bpms_biz_apply_order_common t1
left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
left join tmp_check_opinion_rank_2 tco_r2 on t1.flow_instance_id = tco_r2.PROC_INST_ID_
WHERE t2.PROC_DEF_KEY_ IN (
  'bizApplyFlowCash_v2',
  'bizApplyFlowIns_v2',
  'bizApply_mortgage',
  'bizApplyTransition',
  'bizApply_mfb', 'bizApplyFlowInsurance_v2'
)

union all

select
t1.apply_no,
t2.PROC_DEF_KEY_ ,
t1.product_version,
t1.flow_instance_id,
tco.Investigate_user f_sc_date ,
tco.Investigate_status f_sc_status ,
tco.Investigate_opinion f_sc_opinion ,
tco.Investigate_user f_sc_user_name,
tco_r2.Investigate_time s_sc_date
FROM ods.ods_bpms_biz_apply_order_common t1
left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
left join tmp_check_opinion_rank_2 tco_r2 on t1.flow_instance_id = tco_r2.PROC_INST_ID_
WHERE t2.PROC_DEF_KEY_ IN (
  'bizApplyFlowCash_v2_5',
  'bizApplyFlowIns_v2_5', 'bizApplyFlowIns_mfb'
)