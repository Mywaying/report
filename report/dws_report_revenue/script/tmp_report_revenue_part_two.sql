create table if not exists dws.tmp_report_revenue_part_two (
  create_date TIMESTAMP,
  company_name STRING,
  product_name STRING,
  product_type STRING,
  product_name_c STRING,
  is_overtime STRING COMMENT '是否超期',
  zt_amount double COMMENT '在途金额',
  fk_amount double COMMENT '放款金额（万元）',
  net_charge_amount double COMMENT '净收费（万元）',
  customer_capital_cost double COMMENT '客户资金成本',
  lc_value double COMMENT '当日利差',
  ljjsryg_amount double COMMENT '当日累计净收入预估（万元）',
  drlrfy_amount double COMMENT '当日录入返佣金额',
  drzjcb_amount DOUBLE COMMENT '当日资金成本',
  etl_update_time TIMESTAMP
) STORED AS PARQUET;
insert overwrite table dws.tmp_report_revenue_part_two
select * from dws.tmp_report_revenue_part_two
where etl_update_time <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')
union all
select * from dws.tmp_report_revenue_part_two_add;