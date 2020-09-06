drop table if exists dws.tmp_dws_risk_funnel_agg_p1;
CREATE TABLE dws.tmp_dws_risk_funnel_agg_p1 (
  apply_no string comment "订单编号",
  branch_id string comment "子公司id",
  product_name string comment "产品名称",
  partner_bank_name string comment "合作银行",
  sc_user_name string comment "审查经办人",
  is_missing_materials string comment "是否补件",
  baodan_date timestamp comment "报审日期",
  sc_date timestamp comment "审查时间",
  sc_status string comment "审查结果",
  sc_opinion string comment "审查意见",
  sp_date timestamp comment "审批时间",
  sp_status string comment "审批结果",
  sp_opinion string comment "审批意见",
  sp_user_name string comment "审批经办人",
  end_approval_status string comment "最终审批结果",
  first_mancheck_status string comment "首次审批结果",
  etl_update_time timestamp
);

with tmp_check_opinion as (
   select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask2' then tmp.COMPLETE_TIME_ end ) UserTask2_time  
    ,min(case when tmp.TASK_KEY_ = 'UserTask2' then tmp.AUDITOR_NAME_ end ) UserTask2_user  
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.AUDITOR_NAME_ end ) ApplyCheck_user 
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.COMPLETE_TIME_ end ) ApplyCheck_time 
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.STATUS_ end ) ApplyCheck_status 
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.OPINION_ end ) ApplyCheck_opinion 
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) UserTask3_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.OPINION_ end ) UserTask3_opinion
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.AUDITOR_NAME_ end ) UserTask3_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) UserTask4_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.OPINION_ end ) UserTask4_opinion
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.AUDITOR_NAME_ end ) UserTask4_user
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.ASSIGN_TIME_ end ) Investigate_assign_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) ManCheck_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_status
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.OPINION_ end ) ManCheck_opinion
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.AUDITOR_NAME_ end ) ManCheck_user
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) Investigate_time
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_status
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.OPINION_ end ) Investigate_opinion
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.AUDITOR_NAME_ end ) Investigate_user
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.TASK_KEY_ in ("UserTask2", "ApplyCheck", "UserTask3", "UserTask4", "Investigate", "ManCheck")
    ) tmp
    where rank = 1
    group by tmp.PROC_INST_ID_
),

tmp_check_opinion_desc as (
    select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.AUDITOR_NAME_ end ) UserTask4_user
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) UserTask4_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.OPINION_ end ) UserTask4_opinion
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) ManCheck_time
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_status
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.OPINION_ end ) ManCheck_opinion
    ,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.AUDITOR_NAME_ end ) ManCheck_user
    ,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_status
    ,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_status
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_ desc) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.TASK_KEY_ in ("UserTask4", "ManCheck", "Investigate", "UserTask3")
    ) tmp
    where rank = 1
    group by tmp.PROC_INST_ID_
),

tmp_missing_materials as (
   SELECT
   bmm.apply_no
   ,bmm.id
   ,su.fullname_ create_user_name -- 审查反馈人
   ,bmm.create_time -- 补件通知日期
   FROM (
      select *
      ,row_number() over(PARTITION BY apply_no ORDER BY create_time DESC) rn
      from ods.ods_bpms_biz_missing_materials
   ) bmm
   left join ods.ods_bpms_sys_user su on bmm.create_user_id = su.id_
   where bmm.node_id = "Investigate" and rn = 1
),

tmp_main_opinion as (
  SELECT
  t1.apply_no,
  tco.UserTask2_time baodan_date,
  tco.UserTask2_user baodan_user_name,
  (case when t1.apply_status = "finished"
        and tco.ApplyCheck_user = "系统自动终止"
        then tco.ApplyCheck_time
        else tco.UserTask3_time
  end) sc_date,
  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
        then tco.ApplyCheck_status
        else tco.UserTask3_status
  end) sc_status,
  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_opinion
       else tco.UserTask3_opinion
  end) sc_opinion,
  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_user
       else tco.UserTask3_user
  end) sc_user_name,
  tcod.UserTask4_time sp_date,
  tcod.UserTask4_status sp_status,
  tcod.UserTask4_opinion sp_opinion,
  tcod.UserTask4_user sp_user_name
  FROM ods.ods_bpms_biz_apply_order t1
  left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
  left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
  left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
  WHERE t2.PROC_DEF_KEY_ IN (
    'bizApply_all_cash',
    'bizApply_all_insure'
  )

  union all

  SELECT
  t1.apply_no,
  tco.UserTask3_time baodan_date,
  tco.UserTask3_user baodan_user_name,
  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
        then tco.ApplyCheck_time
        else tco.UserTask4_time
   end)  sc_date,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_status
       else tco.UserTask4_status
  end) sc_status,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_opinion
       else tco.UserTask4_opinion
  end) sc_opinion,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_user
       else tco.UserTask4_user
  end) sc_user_name,
  tcod.UserTask4_time sp_date,
  tcod.UserTask4_status sp_status,
  tcod.UserTask4_opinion sp_opinion,
  tcod.UserTask4_user sp_user_name
  FROM ods.ods_bpms_biz_apply_order t1
  left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
  left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
  left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
  WHERE t2.PROC_DEF_KEY_ IN (
    'bizApply_zztfb'
  )

  union all

  SELECT
  t1.apply_no,
  tco.ApplyCheck_time baodan_date,
  tco.ApplyCheck_user baodan_user_name,
  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
      then tco.ApplyCheck_time
      else tco.ManCheck_time
  end) sc_date,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_status
       else tco.ManCheck_status
  end) sc_status,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_opinion
       else tco.ManCheck_opinion
  end) sc_opinion,

  (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_user
       else tco.ManCheck_user
  end) sc_user_name,
  null sp_date,
  null sp_status,
  null sp_opinion,
  null sp_user_name
  FROM ods.ods_bpms_biz_apply_order_common t1
  left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
  left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
  WHERE t1.product_version = 'v2.0'

  UNION ALL

  SELECT
  t1.apply_no,
  tco.ApplyCheck_time baodan_date,
  tco.ApplyCheck_user baodan_user_name,
  case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_time
       else tco.Investigate_time
  end sc_date,

  case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_status
       else tco.Investigate_status
  end sc_status,

  case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_opinion
       else tco.Investigate_opinion
  end sc_opinion,

  case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
       then tco.ApplyCheck_user
       else tco.Investigate_user
  end sc_user_name,
  tcod.ManCheck_time sp_date,
  tcod.ManCheck_status sp_status,
  tcod.ManCheck_opinion sp_opinion,
  tcod.ManCheck_user sp_user_name
  FROM ods.ods_bpms_biz_apply_order_common t1
  left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
  left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
  WHERE t1.product_version = 'v2.5'
),

tmp_max_approval_status as (

  select 
   bao.apply_no 
   ,case when
      nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_user
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_status, opid.Investigate_status)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_status
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then opid.Investigate_status
         end  -- 审查结果
      ) = "agree" then  "同意"
    when  nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_user
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_status, opid.Investigate_status)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_status
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then opid.Investigate_status
         end  -- 审查结果
      ) = "manual_end" then "人工终止"
    when nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_user
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_status, opid.Investigate_status)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_status
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then opid.Investigate_status
         end  -- 审查结果
      ) = "awaiting_check" then "待审批"

    when nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_user
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_status, opid.Investigate_status)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_user   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_status
             when bao.product_version = 'v2.0' then opid.ManCheck_status
             when bao.product_version = 'v2.5' then opid.Investigate_status
         end  -- 审查结果
      ) = "reject" then "驳回"
   end max_approval_status -- 最终审批结果
   from ods.ods_bpms_biz_apply_order_common bao
   left join tmp_check_opinion_desc opid on bao.flow_instance_id = opid.PROC_INST_ID_
),

tmp_first_mancheck AS (

SELECT * 
FROM (
    SELECT proc_inst_id_,status_,opinion_,
    row_number() over(PARTITION BY proc_inst_id_,task_key_ ORDER BY complete_time_ ASC)rank
    FROM ods.ods_bpms_bpm_check_opinion WHERE task_key_= 'ManCheck'
  )a 
WHERE rank =1
)


insert overwrite table dws.tmp_dws_risk_funnel_agg_p1
select 
bao.apply_no 
,bao.branch_id
,bao.product_name
,bao.partner_bank_name
,case when rod.max_approval_status = '待审批' and a.is_spbj = '是' then tmm.create_user_name else tmo.`sc_user_name` end sc_user_name
,case when tmm.id is not null then '是' else "否" end is_missing_materials  -- 是否补件
,tmo.baodan_date -- 报审日期
,case when rod.max_approval_status = '待审批' and a.is_spbj = '是' then tmm.create_time else tmo.`sc_date` end sc_date
,case when rod.max_approval_status = '待审批' and a.is_spbj = '是' then rod.max_approval_status else tmo.`sc_status` end sc_status
,tmo.sc_opinion -- 审查意见
,tmo.sp_date -- 审批时间
,tmo.sp_status -- 审批结果
,tmo.sp_opinion -- 审批意见
,tmo.sp_user_name -- 审批经办人
,tmas.max_approval_status end_approval_status -- 最终审批结果
,CASE WHEN tfm.status_ = 'reject' THEN '驳回'
      WHEN tfm.status_ = 'agree' THEN '同意'
      WHEN tfm.status_ = 'awaiting_check' THEN '待审批'
      WHEN tfm.status_ = 'manual_end' THEN '终止'
      ELSE tfm.status_
 END first_mancheck_status
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from ods.ods_bpms_biz_apply_order_common bao
left join dwd.dwd_report_risk_node_info a  on bao.apply_no = a.apply_no 
LEFT JOIN dwd.dwd_orders_ex rod ON bao.apply_no = rod.apply_no 
left join tmp_missing_materials tmm on tmm.apply_no = bao.apply_no
left join tmp_main_opinion tmo on bao.apply_no = tmo.apply_no
left join tmp_max_approval_status tmas on bao.apply_no = tmas.apply_no
left join tmp_first_mancheck tfm on bao.flow_instance_id = tfm.proc_inst_id_ 
where tmo.baodan_date is not null and tmo.sc_user_name is not null;