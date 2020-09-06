set hive.execution.engine=spark;
drop table if exists dws.dws_report_revenue_x;
create table if not exists dws.dws_report_revenue_x (
  create_date timestamp ,
  company_name string ,
  qdjl_name string  COMMENT '渠道经理',
  sctd_name string  COMMENT '市场团队',
  product_type string COMMENT '产品类型',
  product_name string ,
  product_name_c string ,
  is_overtime string  COMMENT '是否超期',
  fk_amount double  COMMENT '当日放款金额',
  m_fk_amount double  COMMENT '当月放款金额',
  net_charge_amount double  COMMENT '当日净收费',
  m_net_charge_amount double  COMMENT '当月净收费',
  zt_amount double  COMMENT '当日在途余额',
  customer_capital_cost double  COMMENT '当日客户资金成本',
  lc_value double  COMMENT '当日利差',
  ljjsryg_amount double  COMMENT '当日累计净收入预估',
  avg_zt_amount double  COMMENT '当月日均在途余额',
  m_avg_rlc_value double  COMMENT '当月平均日利差',
  m_ljjsryg_amount double  COMMENT '当月净收入预估',
  drzjcb_amount double  COMMENT '当日资金成本',
  drlrfy_amount double  COMMENT '当日录入返佣金额',
  m_drlrfy_amount double  COMMENT '当月录入返佣金额',
  m_drzjcb_amount double  COMMENT '当月资金成本'
);

with tmp_month as (
  select
  b.company_name, b.qdjl_name, b.sctd_name, b.product_type, b.product_name_c, b.product_name, b.is_overtime
  ,sum(b.fk_amount) m_fk_amount
  ,sum(b.zt_amount) m_zt_amount
  ,sum(b.ljjsryg_amount) m_ljjsryg_amount
  ,sum(b.drzjcb_amount) m_drzjcb_amount
  from dws.tmp_report_revenue_part_two_x b
  where b.create_date >= DATE_FORMAT(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1),"yyyy-MM-01")
  group by b.company_name, b.qdjl_name, b.sctd_name, b.product_type, b.product_name_c, b.product_name, b.is_overtime
),

tmp_csc1 as (

    select
    apply_no, to_date(trans_day) trans_day, sum(trans_money) trans_money
    from ods.ods_bpms_c_cost_trade cct
    where cct.trans_type = 'CSC1'
    group by apply_no, to_date(trans_day)
),

tmp_csd1 as (

    select
    apply_no, to_date(trans_day) trans_day, sum(trans_money) trans_money
    from ods.ods_bpms_c_cost_trade cct
    where cct.trans_type = 'CSD1'   and cct.trade_status = '1'
    group by apply_no, to_date(trans_day)
),

tmp_charge_amount as (

    select
    a.company_name
    ,a.qdjl_name
    ,a.sctd_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,b.trans_day create_date
    ,a.is_overtime
    ,nvl(sum(b.trans_money), 0) charge_amount
    ,0 return_premium_amount
    from tmp_csc1 as b
    left join dws.tmp_report_revenue_part_one as a on a.apply_no = b.apply_no

    where b.trans_day >= date_format(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), "yyyy-MM-01") and b.trans_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
    GROUP BY
    a.company_name
    ,a.qdjl_name
    ,a.sctd_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,a.is_overtime
    ,b.trans_day

    union all

    select
    a.company_name
    ,a.qdjl_name
    ,a.sctd_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,b.trans_day create_date
    ,a.is_overtime
    ,0 charge_amount
    ,nvl(sum(b.trans_money), 0) return_premium_amount
    from tmp_csd1 as b
    left join dws.tmp_report_revenue_part_one as a on a.apply_no = b.apply_no

    where b.trans_day >= date_format(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), "yyyy-MM-01") and b.trans_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
    GROUP BY
    a.company_name
    ,a.qdjl_name
    ,a.sctd_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,a.is_overtime
    ,b.trans_day
),

tmp_lrfy as (

    select  irpo.company_name, irpo.qdjl_name, irpo.sctd_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime
    ,sum(s_bl.humanpool_rebate) hsfy_amount
    ,0 fpfy_amount
    ,0 dsdffy_amount
    from ods.ods_bpms_biz_ledger_pay s_bl
    left join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
    where s_bl.humanpool_payment_day >= date_format(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), "yyyy-MM-01")  and s_bl.humanpool_payment_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
    group by irpo.qdjl_name, irpo.sctd_name, irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime

    union all
    -- 当日录入返佣金额-发票报销
    select irpo.company_name, irpo.qdjl_name, irpo.sctd_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime
    ,0 hsfy_amount
    ,sum(s_bl.invoice_rebate) fpfy_amount
    ,0 dsdffy_amount
    from ods.ods_bpms_biz_ledger_invoice s_bl
    left join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
    where s_bl.invoice_submit_day >= date_format(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), "yyyy-MM-01") and s_bl.invoice_submit_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
    group by irpo.qdjl_name, irpo.sctd_name, irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime

    union all
    -- 当日录入返佣金额-发票报销
    select irpo.company_name, irpo.qdjl_name, irpo.sctd_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime
    ,0 hsfy_amount
    ,0 fpfy_amount
    ,sum(replace_turn_out) dsdffy_amount
    from ods.ods_bpms_biz_ledger_instead s_bl
    left join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
    where s_bl.replace_turn_day >= date_format(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), "yyyy-MM-01") and s_bl.replace_turn_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
    group by irpo.qdjl_name, irpo.sctd_name, irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime
)

insert overwrite table dws.dws_report_revenue_x
select
date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
,a.company_name, a.qdjl_name, a.sctd_name, a.product_type ,a.product_name ,a.product_name_c ,a.is_overtime
,cast(sum(fk_amount) as double) fk_amount
,cast(sum(m_fk_amount) as double) m_fk_amount
,cast(sum(net_charge_amount) as double) net_charge_amount
,cast(sum(m_net_charge_amount) as double) m_net_charge_amount
,cast(sum(zt_amount) as double) zt_amount
,cast(sum(customer_capital_cost) as double) customer_capital_cost
,cast(sum(lc_value) as double) lc_value
,cast(sum(ljjsryg_amount) as double) ljjsryg_amount
,cast(sum(avg_zt_amount) as double) avg_zt_amount
,cast(sum(m_avg_rlc_value) as double) m_avg_rlc_value
,cast(sum(m_ljjsryg_amount) as double) m_ljjsryg_amount
,cast(sum(drzjcb_amount) as double) drzjcb_amount
,cast(sum(drlrfy_amount) as double) drlrfy_amount
,cast(sum(m_drlrfy_amount) as double) m_drlrfy_amount
,cast(sum(m_drzjcb_amount) as double) m_drzjcb_amount
from (
    SELECT
    a.create_date
    ,a.company_name
    ,a.qdjl_name
    ,a.sctd_name
    ,a.product_type
    ,a.product_name
    ,a.product_name_c
    ,a.is_overtime
    ,a.fk_amount
    ,tm.m_fk_amount
    ,a.net_charge_amount
    ,0 m_net_charge_amount
    ,a.zt_amount
    ,a.customer_capital_cost
    ,a.lc_value
    ,a.ljjsryg_amount
    ,tm.m_zt_amount/day(a.create_date) avg_zt_amount

    ,(
      tm.m_ljjsryg_amount
      / tm.m_zt_amount/day(a.create_date)
      / day(a.create_date)*360
    ) m_avg_rlc_value

    ,tm.m_ljjsryg_amount
    ,a.drzjcb_amount
    ,a.drlrfy_amount
    ,0 m_drlrfy_amount
    ,tm.m_drzjcb_amount

    FROM
    dws.tmp_report_revenue_part_two_x AS a
    left join tmp_month tm on a.company_name = tm.company_name
                          and a.qdjl_name = tm.qdjl_name
                          and a.sctd_name = tm.sctd_name
                          and a.product_name = tm.product_name
                          and a.product_type = tm.product_type
                          and a.product_name_c = tm.product_name_c
                          and a.is_overtime = tm.is_overtime

    where a.create_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)

    union all

    select
    date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
    ,a.company_name, a.qdjl_name, a.sctd_name, a.product_type ,a.product_name ,a.product_name_c ,a.is_overtime
    ,0 fk_amount
    ,0 m_fk_amount
    ,0 net_charge_amount
    ,sum(charge_amount) - sum(return_premium_amount) m_net_charge_amount
    ,0 zt_amount
    ,0 customer_capital_cost
    ,0 lc_value
    ,0 ljjsryg_amount
    ,0 avg_zt_amount
    ,0 m_avg_rlc_value
    ,0 m_ljjsryg_amount
    ,0 drzjcb_amount
    ,0 drlrfy_amount
    ,0 m_drlrfy_amount
    ,0 m_drzjcb_amount
    from tmp_charge_amount as a
    group by a.qdjl_name, a.sctd_name, a.company_name ,a.product_name ,a.product_type ,a.product_name_c ,a.is_overtime

    union all

    select
    date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
    ,a.company_name, a.qdjl_name, a.sctd_name, a.product_type ,a.product_name ,a.product_name_c ,a.is_overtime
    ,0 fk_amount
    ,0 m_fk_amount
    ,0 net_charge_amount
    ,0 m_net_charge_amount
    ,0 zt_amount
    ,0 customer_capital_cost
    ,0 lc_value
    ,0 ljjsryg_amount
    ,0 avg_zt_amount
    ,0 m_avg_rlc_value
    ,0 m_ljjsryg_amount
    ,0 drzjcb_amount
    ,0 drlrfy_amount
    ,nvl(sum(a.hsfy_amount),0) + nvl(sum(a.fpfy_amount), 0) + nvl(sum(a.dsdffy_amount), 0) m_drlrfy_amount
    ,0 m_drzjcb_amount
    from tmp_lrfy as a
    group by a.qdjl_name, a.sctd_name, a.company_name ,a.product_name ,a.product_type ,a.product_name_c ,a.is_overtime
) as a
where a.product_name is not null
group by a.qdjl_name, a.sctd_name, a.company_name ,a.product_name ,a.product_type ,a.product_name_c ,a.is_overtime