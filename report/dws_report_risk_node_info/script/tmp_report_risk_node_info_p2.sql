set hive.execution.engine=spark;
drop table if exists dwd.tmp_report_risk_node_info_p2;
create table if not exists dwd.tmp_report_risk_node_info_p2 (
  `apply_no` string ,
  `xtzrgsc` string  COMMENT '是否系统转人工审查',
  `zrgyy` string COMMENT '转人工原因',
  `rgsc_date` timestamp  COMMENT '人工审查日期',
  `rgsc_time` string  COMMENT '人工审查时间',
  `rgsc_user` string  COMMENT '人工审查人员',
  `rgsc_status` string  COMMENT '人工审查结果',
  `rgsc_opinion` string COMMENT '人工审查意见'
);

with tmp_check_opinion as (

    select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'InputInfoComplete' then tmp.STATUS_ end ) InputInfoComplete_status
    ,min(case when tmp.TASK_KEY_ = 'InputInfoComplete' then tmp.OPINION_ end ) InputInfoComplete_opinion
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
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) UserTask3_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) UserTask4_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) ManCheck_time
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) Investigate_time
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.COMPLETE_TIME_ is not null and b.TASK_KEY_ in ('UserTask3', 'UserTask4', 'ManCheck', 'Investigate')
    ) tmp
    where rank = 2
    group by tmp.PROC_INST_ID_
),

tmp_s_s as (

    SELECT
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.AUDITOR_NAME_ end) UserTask4_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.AUDITOR_NAME_ end) UserTask3_user
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.AUDITOR_NAME_ end) ManCheck_user
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.AUDITOR_NAME_ end) Investigate_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end) UserTask4_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end) UserTask3_status
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end) ManCheck_status
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end) Investigate_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.OPINION_ end) UserTask4_opinion
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.OPINION_ end) UserTask3_opinion
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.OPINION_ end) ManCheck_opinion
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.OPINION_ end) Investigate_opinion
    FROM ods.ods_bpms_bpm_check_opinion tmp
    join dwd.tmp_report_risk_node_info_p1 t on tmp.PROC_INST_ID_ = t.flow_instance_id and tmp.COMPLETE_TIME_ = t.s_sc_date
    group by tmp.PROC_INST_ID_
)

insert overwrite table dwd.tmp_report_risk_node_info_p2
select
t.apply_no
,case when t.f_sc_status = "agree"  and t.f_sc_user_name like "%系统%" and tco.InputInfoComplete_status = "reject"
      then "是"
      when t.f_sc_status = "reject" and t.f_sc_user_name not like "%系统%" then "否"
 end xtzrgsc --  是否系统转人工审查

,case when t.f_sc_status = "agree"  and t.f_sc_user_name like "%系统%" and tco.InputInfoComplete_status = "reject"
      then tco.InputInfoComplete_opinion
      else null
 end zrgyy  --  转人工原因

, case when t.PROC_DEF_KEY_ = "bizApply_zztfb"
       then to_date(tco_r2.UserTask4_time)
       when t.product_version in ("v1.5", "v1.0")
       then to_date(tco_r2.UserTask3_time)
       when t.product_version = "v2.0"
       then to_date(tco_r2.ManCheck_time)
       when t.product_version = "v2.5"
       then to_date(tco_r2.Investigate_time)
end  rgsc_date --  人工审查日期

,case  when t.PROC_DEF_KEY_ = "bizApply_zztfb"
       then DATE_FORMAT(tco_r2.UserTask4_time,'HH:mm:ss')
       when t.product_version in ("v1.5", "v1.0")
       then DATE_FORMAT(tco_r2.UserTask3_time,'HH:mm:ss')
       when t.product_version = "v2.0"
       then DATE_FORMAT(tco_r2.ManCheck_time,'HH:mm:ss')
       when t.product_version = "v2.5"
       then  DATE_FORMAT(tco_r2.Investigate_time,'HH:mm:ss')
end  rgsc_time --  人工审查时间

,case when t.PROC_DEF_KEY_ = "bizApply_zztfb"
      then tss.UserTask4_user
      when t.product_version in ("v1.5", "v1.0")
      then  tss.UserTask3_user
      when t.product_version = "v2.0"
      then tss.ManCheck_user
      when t.product_version = "v2.5"
      then  tss.Investigate_user
end  rgsc_user --  人工审查人员

,case when t.PROC_DEF_KEY_ = "bizApply_zztfb"
      then tss.UserTask4_status
      when t.product_version in ("v1.5", "v1.0")
      then tss.UserTask3_status
      when t.product_version = "v2.0"
      then tss.ManCheck_status
      when t.product_version = "v2.5"
      then tss.Investigate_status
end  rgsc_status --  人工审查结果

,case when t.PROC_DEF_KEY_ = "bizApply_zztfb"
      then tss.UserTask4_opinion
      when t.product_version in ("v1.5", "v1.0")
      then tss.UserTask3_opinion
      when t.product_version = "v2.0"
      then tss.ManCheck_opinion
      when t.product_version = "v2.5"
      then tss.Investigate_opinion
end  rgsc_opinion --  人工审查意见

from dwd.tmp_report_risk_node_info_p1 t
left join tmp_check_opinion tco on t.flow_instance_id = tco.PROC_INST_ID_
left join tmp_check_opinion_rank_2 tco_r2 on t.flow_instance_id = tco_r2.PROC_INST_ID_
left join tmp_s_s tss on t.flow_instance_id = tss.PROC_INST_ID_