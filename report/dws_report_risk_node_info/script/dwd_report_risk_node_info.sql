set hive.execution.engine=spark;
drop table if exists dwd.dwd_report_risk_node_info;
create table if not exists dwd.dwd_report_risk_node_info (
  `version_no` string ,
  `apply_no` string ,
  `partner_bank_name` string COMMENT '合作银行',
  `company_name` string  COMMENT '公司名称',
  `product_name` string  COMMENT '产品名称',
  `tail_release_node` string  COMMENT '放款节点',
  `channel_man` string COMMENT '渠道经理',
  `seller_name` string COMMENT '借款人',
  

  `partner_name` string  COMMENT '合作机构',
  `baodan_date` timestamp  COMMENT '报单日期',
  `baodan_time` string  COMMENT '报单时间',
  `baodan_user_name` string COMMENT '报单人员',
  `sc_date` timestamp  COMMENT '审查日期',
  `sc_time` string  COMMENT '审查时间',
  `sc_status` string COMMENT '审查结果',
  `sc_opinion` string COMMENT '审查意见',
  `sc_user_name` string  COMMENT '审查人员',
  `sc_rel_use_time` bigint,
  `sp_date` timestamp  COMMENT '审批日期',
  `sp_time` string  COMMENT '审批时间',
  `sp_status` string COMMENT '审批结果',
  `sp_opinion` string COMMENT '审批意见',
  `sp_user_name` string  COMMENT '审批人员',
  `sp_rel_use_time` bigint,
  `first_spbj_date` timestamp  COMMENT '第一次审批补件时间',
  `first_spbj_time` string ,
  `materials_full_date` timestamp COMMENT '确认录入完成时间',
  `input_end_date` timestamp COMMENT '确认录入完成时间',
  `standard_sc_time` timestamp ,
  `standard_spbj_time` string,
  `standard_sp_time` timestamp ,
  `standard_baodan_time` timestamp ,
  `sc_use_time` bigint comment '审查处理时间(小时)',
  `sp_use_time` bigint comment '审批处理时间(小时)',
  `spbj_use_time` double,
  `risk_level` string COMMENT '风险等级',
  `is_spbj` string ,
  `ydkyh_name` string COMMENT '原贷款机构',
  `xdkyh_name` string COMMENT '新贷款机构',
  `mqspsx` string ,
  `ydkjkrlx` string  COMMENT '原贷款借款人类型',
  `xdkjkrlx` string  COMMENT '新贷款借款人类型',
  `rule_level` string COMMENT '难易程度',
  `borrow_amount` double  COMMENT '借款金额',
  `scsd_date` timestamp  COMMENT '审查锁定时间'
);
with tmp_check_opinion as (
   select
    tmp.PROC_INST_ID_
    ,min(case when tmp.TASK_KEY_ = 'UserTask2' then tmp.COMPLETE_TIME_ end ) UserTask2_time  -- baodan_time
    ,min(case when tmp.TASK_KEY_ = 'UserTask2' then tmp.AUDITOR_NAME_ end ) UserTask2_user  -- baodan_user_name
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.AUDITOR_NAME_ end ) ApplyCheck_user -- sc_user_name
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.COMPLETE_TIME_ end ) ApplyCheck_time -- sc_time
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.STATUS_ end ) ApplyCheck_status -- sc_status
    ,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.OPINION_ end ) ApplyCheck_opinion -- sp_opinion
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
        where b.COMPLETE_TIME_ is not null
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
    from(
      select
        b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_ desc) rank
        from ods.ods_bpms_bpm_check_opinion b
        where b.COMPLETE_TIME_ is not null
    ) tmp
    where rank = 1
    group by tmp.PROC_INST_ID_
),

tmp_biz_missing_materials  as (

    SELECT
    bmm.apply_no
    ,max(id) max_id
    ,min(bmm.create_time) min_create_time
    FROM ods.ods_bpms_biz_missing_materials bmm
    where bmm.node_id = "Investigate"
    group by bmm.apply_no
),

tmp_matter_record as (

    select
    apply_no,
    MAX(bomr.create_time) InputInfoComplete_time,
    Max(bomr.handle_time) InputInfoComplete_h_time
    from ods.ods_bpms_biz_order_matter_record bomr
    where matter_key='InputInfoComplete'
    group by apply_no
),

tmp_ods_bpms_b_product_node as (

  select * from 
  (
    select
    a.*
    ,ROW_NUMBER() OVER(PARTITION BY loan_node_code ORDER BY create_time desc) rn
    from ods.ods_bpms_b_product_node a
  ) as a
  where rn = 1
),

tmp_all as (

    SELECT
     'V1.5' as version_no,
    t1.apply_no apply_no,
    t1.partner_bank_name,
    case when t1.man_check_first = 'Y' then '先审批后面签'
         when t1.man_check_first = 'N' then '先面签后审批'
    end mqspsx,
    t1.product_name product_name_o,
    t1.house_no,
    so_1.NAME_  company_name,
    IF(t2.PROC_DEF_KEY_ = 'bizApply_zztfb','郑州提放保',t1.product_name) product_name,
    nvl(sd_1.name_, bpn.loan_node_name) tail_release_node , -- 放款节点,
    t1.sales_user_name channel_man,
    t1.seller_name seller_name,
    t1.partner_insurance_name partner_name,
    DATE_FORMAT(tco.UserTask2_time,'yyyy-MM-dd') baodan_date,
    DATE_FORMAT(tco.UserTask2_time,'HH:mm:ss') baodan_time,
    tco.UserTask2_user baodan_user_name,
    (case when t1.apply_status = "finished"
          and tco.ApplyCheck_user = "系统自动终止"
          then DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd')
          else DATE_FORMAT(tco.UserTask3_time,'yyyy-MM-dd')
    end) sc_date,
    (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
          then DATE_FORMAT(tco.ApplyCheck_time,'HH:mm:ss')
          else DATE_FORMAT(tco.UserTask3_time,'HH:mm:ss')
    end) sc_time,
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
         else tco.ApplyCheck_user
    end) sc_user_name,
    null sc_rel_use_time,
    DATE_FORMAT(tcod.UserTask4_time,'yyyy-MM-dd') sp_date,
    DATE_FORMAT(tcod.UserTask4_time,'HH:mm:ss') sp_time,
    tcod.UserTask4_status sp_status,
    tcod.UserTask4_opinion sp_opinion,
    tcod.UserTask4_user sp_user_name,
    null sp_rel_use_time,
    null first_spbj_date,
    null first_spbj_time,
    tco.Investigate_assign_time scsd_date --  审查锁定时间
    FROM ods.ods_bpms_biz_apply_order t1
    left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
    left join ods.ods_bpms_biz_isr_mixed t3 on t1.apply_no = t3.apply_no
    left join ods.ods_bpms_biz_fee_summary t4 on t1.apply_no = t4.apply_no
    left join ods.ods_bpms_sys_org so_1 on t1.branch_id = so_1.CODE_
    left join ods.ods_bpms_sys_dic sd_1 on sd_1.key_=t3.tail_release_node and sd_1.type_id_ = "10000047750219"
    left join tmp_ods_bpms_b_product_node bpn on bpn.loan_node_code=t3.tail_release_node 
    left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
    left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
    WHERE t2.PROC_DEF_KEY_ IN (
      'bizApply_all_cash',
      'bizApply_all_insure'
    )

    union all

    SELECT
     'V1.5' as version_no,
    t1.apply_no apply_no,
    t1.partner_bank_name,
    case when t1.man_check_first = 'Y' then '先审批后面签'
         when t1.man_check_first = 'N' then '先面签后审批'
    end mqspsx,
    t1.product_name product_name_o,
    t1.house_no,
    so_1.NAME_  company_name,
    IF (t2.PROC_DEF_KEY_ = 'bizApply_zztfb','郑州提放保',t1.product_name) product_name,
    nvl(sd_1.name_, bpn.loan_node_name) tail_release_node , --放款节点,
    t1.sales_user_name channel_man,
    t1.seller_name seller_name,
    t1.partner_insurance_name partner_name,
    DATE_FORMAT(tco.UserTask3_time,'yyyy-MM-dd') baodan_date,
    DATE_FORMAT(tco.UserTask3_time,'HH:mm:ss') baodan_time,
    tco.UserTask3_user baodan_user_name,
    (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
          then DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd')
          else DATE_FORMAT(tco.UserTask4_time,'yyyy-MM-dd')
     end)  sc_date,

    (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
         then DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd')
         else DATE_FORMAT(tco.UserTask4_time,'HH:mm:ss')
    end) sc_time,

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
    null sc_rel_use_time,
    DATE_FORMAT(tcod.UserTask4_time,'yyyy-MM-dd') sp_date,
    DATE_FORMAT(tcod.UserTask4_time,'HH:mm:ss') sp_time,
    tcod.UserTask4_status sp_status,
    tcod.UserTask4_opinion sp_opinion,
    tcod.UserTask4_user sp_user_name,
    null sp_rel_use_time,
    null first_spbj_date,
    null first_spbj_time,
    tco.Investigate_assign_time scsd_date --  审查锁定时间
    FROM ods.ods_bpms_biz_apply_order t1
    left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
    left join ods.ods_bpms_biz_isr_mixed t3 on t1.apply_no = t3.apply_no
    left join ods.ods_bpms_biz_fee_summary t4 on t1.apply_no = t4.apply_no
    left join ods.ods_bpms_sys_org so_1 on t1.branch_id = so_1.CODE_
    left join ods.ods_bpms_sys_dic sd_1 on sd_1.key_=t3.tail_release_node and sd_1.type_id_ = "10000047750219"
    left join tmp_ods_bpms_b_product_node bpn on bpn.loan_node_code=t3.tail_release_node
    left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
    left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
    WHERE t2.PROC_DEF_KEY_ IN (
      'bizApply_zztfb'
    )

    union all

    SELECT
     'V2.0' as version_no,
      t1.apply_no apply_no,
      t1.partner_bank_name,
      case when t1.man_check_first = 'Y' then '先审批后面签'
           when t1.man_check_first = 'N' then '先面签后审批'
      end mqspsx,
      t1.product_name product_name_o,
      t1.house_no,
      so_1.NAME_  company_name,
      IF(t2.PROC_DEF_KEY_ = 'bizApply_zztfb','郑州提放保',t1.product_name) product_name,
      nvl(sd_1.name_, bpn.loan_node_name) tail_release_node , -- 放款节点,
      t1.sales_user_name channel_man,
      t1.seller_name seller_name,
      t1.partner_insurance_name partner_name,
     --   当单子被系统拒绝时，biz_order_matter_record不会存储相关的数据， 只有bpm_check_opinion表中存在
      DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd') baodan_date,
      DATE_FORMAT(tco.ApplyCheck_time,'HH:mm:ss') baodan_time,
      tco.ApplyCheck_user baodan_user_name,

     (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
          then DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd')
          else DATE_FORMAT(tco.ManCheck_time,'yyyy-MM-dd')
     end) sc_date,

    (case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
         then DATE_FORMAT(tco.ApplyCheck_time,'HH:mm:ss')
         else DATE_FORMAT(tco.ManCheck_time,'HH:mm:ss')
    end) sc_time,

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

    NULL sc_rel_use_time,

    null sp_date,
    null sp_time,
    null sp_status,
    null sp_opinion,
    null sp_user_name,
    NULL sp_rel_use_time,
    null first_spbj_date,
    null first_spbj_time,
    tco.Investigate_assign_time scsd_date --  审查锁定时间
    FROM ods.ods_bpms_biz_apply_order t1
    left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
    left join ods.ods_bpms_biz_isr_mixed t3 on t1.apply_no = t3.apply_no
    left join ods.ods_bpms_biz_fee_summary t4 on t1.apply_no = t4.apply_no
    left join ods.ods_bpms_sys_org so_1 on t1.branch_id = so_1.CODE_
    left join ods.ods_bpms_sys_dic sd_1 on sd_1.key_=t3.tail_release_node and sd_1.type_id_ = "10000047750219"
    left join tmp_ods_bpms_b_product_node bpn on bpn.loan_node_code=t3.tail_release_node 
    left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
    left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
    WHERE t2.PROC_DEF_KEY_ IN (
      'bizApplyFlowCash_v2',
      'bizApplyFlowIns_v2',
      'bizApply_mortgage',
      'bizApplyTransition',
      'bizApply_mfb', 'bizApplyFlowInsurance_v2'

    )

    UNION ALL

    SELECT
     'V2.5' as version_no,
      t1.apply_no apply_no,
      t1.partner_bank_name,
      case when t1.man_check_first = 'Y' then '先审批后面签'
           when t1.man_check_first = 'N' then '先面签后审批'
      end mqspsx,
      t1.product_name product_name_o,
      t1.house_no,
      so_1.NAME_  company_name,
      IF (t2.PROC_DEF_KEY_ = 'bizApply_zztfb','郑州提放保',t1.product_name) product_name,
      nvl(sd_1.name_, bpn.loan_node_name) tail_release_node , --放款节点,
      t1.sales_user_name channel_man,
      t1.seller_name seller_name,
      t1.partner_insurance_name partner_name,
     --   当单子被系统拒绝时，biz_order_matter_record不会存储相关的数据， 只有bpm_check_opinion表中存在
      DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd') baodan_date,
      DATE_FORMAT(tco.ApplyCheck_time,'HH:mm:ss') baodan_time,

      tco.ApplyCheck_user baodan_user_name,

      case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
           then DATE_FORMAT(tco.ApplyCheck_time,'yyyy-MM-dd')
           else DATE_FORMAT(tco.Investigate_time,'yyyy-MM-dd')
      end sc_date,

      case when t1.apply_status = "finished" and tco.ApplyCheck_user = "系统自动终止"
           then DATE_FORMAT(tco.ApplyCheck_time,'HH:mm:ss')
           else DATE_FORMAT(tco.Investigate_time,'HH:mm:ss')
      end sc_time,

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
      NULL sc_rel_use_time,
      DATE_FORMAT(tcod.ManCheck_time,'yyyy-MM-dd') sp_date,
      DATE_FORMAT(tcod.ManCheck_time,'HH:mm:ss') sp_time,
      tcod.ManCheck_status sp_status,
      tcod.ManCheck_opinion sp_opinion,
      tcod.ManCheck_user sp_user_name,
      NULL sp_rel_use_time,
      to_date(tbmm.min_create_time) first_spbj_date,
      date_format(tbmm.min_create_time, "HH:mm:ss") first_spbj_time,
      tco.Investigate_assign_time scsd_date --  审查锁定时间
    FROM ods.ods_bpms_biz_apply_order t1
    left join ods.ods_bpms_bpm_pro_inst t2 on t1.flow_instance_id = t2.ID_
    left join ods.ods_bpms_biz_isr_mixed t3 on t1.apply_no = t3.apply_no
    left join ods.ods_bpms_biz_fee_summary t4 on t1.apply_no = t4.apply_no
    left join ods.ods_bpms_sys_org so_1 on t1.branch_id = so_1.CODE_
    left join ods.ods_bpms_sys_dic sd_1 on sd_1.key_=t3.tail_release_node and sd_1.type_id_ = "10000047750219"
    left join tmp_ods_bpms_b_product_node bpn on bpn.loan_node_code=t3.tail_release_node 
    left join tmp_check_opinion tco on t1.flow_instance_id = tco.PROC_INST_ID_
    left join tmp_check_opinion_desc tcod on t1.flow_instance_id = tcod.PROC_INST_ID_
    left join tmp_biz_missing_materials tbmm on t1.apply_no = tbmm.apply_no
    WHERE t2.PROC_DEF_KEY_ IN (
      'bizApplyFlowCash_v2_5',
      'bizApplyFlowIns_v2_5', 'bizApplyFlowIns_mfb'
    )
)

insert overwrite table dwd.dwd_report_risk_node_info
select
a.version_no version_no -- 版本号
,a.apply_no apply_no
,a.partner_bank_name -- 合作银行
,a.company_name company_name -- 公司名称
,a.product_name product_name -- 业务类型
,a.tail_release_node tail_release_node -- 放款节点
,a.channel_man channel_man -- 渠道经理
,a.seller_name seller_name -- 借款人
,a.partner_name partner_name -- 机构
,a.baodan_date baodan_date -- 报审日期
,a.baodan_time baodan_time -- 报审时间
,a.baodan_user_name baodan_user_name -- 报审人员
,a.sc_date sc_date -- 审查日期
,a.sc_time sc_time -- 审查时间
,a.sc_status sc_status -- 审查结果
,a.sc_opinion sc_opinion -- 审查意见
,case when a.sc_status = "awaiting_check" then substr(a.sc_opinion, 1, INSTR("已",a.sc_opinion) - 1)
else a.sc_user_name
end sc_user_name -- 审查人员
,a.sc_rel_use_time sc_rel_use_time -- `sc_rel_use_time`
,a.sp_date sp_date -- 审批日期
,a.sp_time sp_time -- 审批时间
,a.sp_status sp_status -- 审批结果
,a.sp_opinion sp_opinion -- 审批意见
,a.sp_user_name sp_user_name -- 审批人员
,a.sp_rel_use_time sp_rel_use_time --
,a.first_spbj_date
,a.first_spbj_time
,tmr.InputInfoComplete_time  materials_full_date --  '确认录入完成时间',
,tmr.InputInfoComplete_h_time input_end_date --  '确认录入完成时间',
,(CASE
  WHEN  DATE_FORMAT(concat(sc_date," ", sc_time),'H') <9
  THEN CONCAT(DATE_FORMAT(concat(sc_date," ", sc_time),'yyyy-MM-dd'),' 09:00:00')
  WHEN  DATE_FORMAT(concat(sc_date," ", sc_time),'H') BETWEEN 12 AND 13
  THEN CONCAT(DATE_FORMAT(concat(sc_date," ", sc_time),'yyyy-MM-dd'),' 14:00:00')
  WHEN  DATE_FORMAT(concat(sc_date," ", sc_time),'H') >= 18
  THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(sc_date," ", sc_time),1),'yyyy-MM-dd'),' 09:00:00')
  ELSE concat(sc_date," ", sc_time)
END) AS standard_sc_time

,(CASE
  WHEN  DATE_FORMAT(concat(first_spbj_date," ", first_spbj_time),'H') <9
  THEN CONCAT(DATE_FORMAT(concat(first_spbj_date," ", first_spbj_time),'yyyy-MM-dd'),' 09:00:00')
  WHEN  DATE_FORMAT(concat(first_spbj_date," ", first_spbj_time),'H') BETWEEN 12 AND 13
  THEN CONCAT(DATE_FORMAT(concat(first_spbj_date," ", first_spbj_time),'yyyy-MM-dd'),' 14:00:00')
  WHEN  DATE_FORMAT(concat(first_spbj_date," ", first_spbj_time),'H') >= 18
  THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(first_spbj_date," ", first_spbj_time),1),'yyyy-MM-dd'),' 09:00:00')
  ELSE concat(first_spbj_date," ", first_spbj_time)
END) AS standard_spbj_time

,(CASE
  WHEN  DATE_FORMAT(concat(sp_date," ", sp_time),'H') <9
  THEN CONCAT(DATE_FORMAT(concat(sp_date," ", sp_time),'yyyy-MM-dd'),' 09:00:00')
  WHEN  DATE_FORMAT(concat(sp_date," ", sp_time),'H') BETWEEN 12 AND 13
  THEN CONCAT(DATE_FORMAT(concat(sp_date," ", sp_time),'yyyy-MM-dd'),' 14:00:00')
  WHEN  DATE_FORMAT(concat(sp_date," ", sp_time),'H') >= 18
  THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(sp_date," ", sp_time),1),'yyyy-MM-dd'),' 09:00:00')
  ELSE concat(sp_date," ", sp_time)
END) AS standard_sp_time

,(CASE
  WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') <9
  THEN CONCAT(DATE_FORMAT(concat(baodan_date," ", baodan_time),'yyyy-MM-dd'),' 09:00:00')
  WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') BETWEEN 12 AND 13
  THEN CONCAT(DATE_FORMAT(concat(baodan_date," ", baodan_time),'yyyy-MM-dd'),' 14:00:00')
  WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') >= 18
  THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(baodan_date," ", baodan_time),1),'yyyy-MM-dd'),' 09:00:00')
  ELSE concat(baodan_date," ", baodan_time)
END) AS standard_baodan_time

,round((unix_timestamp(concat(sc_date," ", sc_time))
- unix_timestamp(
    CASE
      WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') <9
      THEN CONCAT(DATE_FORMAT(concat(baodan_date," ", baodan_time),'yyyy-MM-dd'),' 09:00:00')
      WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') >= 18
      THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(baodan_date," ", baodan_time),1),'yyyy-MM-dd'),' 09:00:00')
      ELSE concat(baodan_date," ", baodan_time)
      END)
)/3600, 0) sc_use_time  --  审查处理时间(小时)

,round((unix_timestamp(concat(sp_date," ", sp_time))
- unix_timestamp(
    CASE
        WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') <9
        THEN CONCAT(DATE_FORMAT(concat(baodan_date," ", baodan_time),'yyyy-MM-dd'),' 09:00:00')
        WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') >= 18
        THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(baodan_date," ", baodan_time),1),'yyyy-MM-dd'),' 09:00:00')
        ELSE concat(baodan_date," ", baodan_time)
       end)
)/3600, 2) sp_use_time  --  审批处理时间(小时)

,round((unix_timestamp(concat(first_spbj_date," ", first_spbj_time))
- unix_timestamp(
    CASE
        WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') <9
        THEN CONCAT(DATE_FORMAT(concat(baodan_date," ", baodan_time),'yyyy-MM-dd'),' 09:00:00')
        WHEN  DATE_FORMAT(concat(baodan_date," ", baodan_time),'H') >= 18
        THEN CONCAT(DATE_FORMAT(DATE_ADD(concat(baodan_date," ", baodan_time),1),'yyyy-MM-dd'),' 09:00:00')
        ELSE concat(baodan_date," ", baodan_time)
       END)
)/3600, 2)  spbj_use_time
,bim.risk_level --  风险等级
,(case when tbmm.max_id is not null then '是'
      else "否"
 end) is_spbj
,(case when a.product_name_o like "%赎楼%" and a.product_name_o not like "%无%"
     then bol.ori_loan_bank_name
  else "-"
 end ) ydkyh_name --  原贷款银行
,bnl.new_loan_bank_name  xdkyh_name  --  新贷款银行
,mqspsx
,case when bol.borrower_type = "PERSONAL" then "个人"
      when bol.borrower_type = "PUBLIC" then "公司"
 end ydkjkrlx --  原贷款seller_name类型
,case when bnl.borrower_type = "PERSONAL" then "个人"
      when bnl.borrower_type = "PUBLIC" then "公司"
 end xdkjkrlx --  新贷款seller_name类型
, case when bim.rule_level = "easy" then "易"
  when bim.rule_level = "mad" then "难"
  end rule_level --  难易程度
,(case when a.product_name not like "%及时贷%" then bnl.biz_loan_amount
       else bfs.borrowing_amount
  end) borrow_amount --  金额
,a.scsd_date --  审查锁定时间
from tmp_all as a
left join ods.ods_bpms_biz_isr_mixed bim on a.apply_no = bim.apply_no
left join tmp_biz_missing_materials tbmm on a.apply_no = tbmm.apply_no
left join (select * from ods.ods_bpms_biz_ori_loan_common where rn = 1) bol on bol.apply_no=a.apply_no
left join (select * from ods.ods_bpms_biz_new_loan_common where rn = 1) bnl on bnl.apply_no=a.apply_no
left join ods.ods_bpms_biz_fee_summary bfs on bfs.apply_no=a.apply_no
left join tmp_matter_record tmr on tmr.apply_no = a.apply_no