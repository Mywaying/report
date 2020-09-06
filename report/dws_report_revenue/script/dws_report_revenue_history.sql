CREATE TABLE if not exists dws.dws_report_revenue_history (
  create_date TIMESTAMP,
  company_name STRING,
  product_type STRING COMMENT '产品类型',
  product_name STRING,
  product_name_c STRING,
  is_overtime STRING COMMENT '是否超期',
  fk_amount double COMMENT '当日放款金额',
  m_fk_amount double COMMENT '当月放款金额',
  net_charge_amount double COMMENT '当日净收费',
  m_net_charge_amount double COMMENT '当月净收费',
  zt_amount double COMMENT '当日在途余额',
  customer_capital_cost double COMMENT '当日客户资金成本',
  lc_value double COMMENT '当日利差',
  ljjsryg_amount double COMMENT '当日累计净收入预估',
  avg_zt_amount double COMMENT '当月日均在途余额',
  m_avg_rlc_value double COMMENT '当月平均日利差',
  m_ljjsryg_amount double COMMENT '当月净收入预估',
  drzjcb_amount double COMMENT '当日资金成本',
  drlrfy_amount double COMMENT '当日录入返佣金额',
  m_drlrfy_amount double COMMENT '当月录入返佣金额',
  m_drzjcb_amount double COMMENT '当月资金成本',
  m_net_premium_amount double COMMENT '当月应加收费-保费',
  m_total_net_amount double COMMENT '当月所有净收费',
  etl_update_time TIMESTAMP
) stored as parquet;
insert overwrite table dws.dws_report_revenue_history
select * from dws.dws_report_revenue_history
where etl_update_time <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')
union all
select * from dws.dws_report_revenue;