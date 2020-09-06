set hive.execution.engine=spark;
use dws;
drop table if exists tmp_archive_order ;
CREATE TABLE if not exists `tmp_archive_order`( `apply_no` string,  `product_id` string, `product_type` string,  `product_name` string, `city_name` string,  `branch_id` string,  `branch_name` string, `jiaofei_amt` double,   `premium_amt` double, `tuifei_amt` double,   `financialarchiveevent_time` string,revenue double,release_amount double COMMENT '放款金额',isjms STRING comment '是否加盟') STORED AS parquet;
with con_t as (
  select
  cct.apply_no
  , SUM(cct.trans_money)  jiaofei_amt -- 缴费
  , sum(premium) premium_amt -- 保费
  from ods.ods_bpms_c_cost_trade cct
  where cct.trans_type = "CSC1"
  group by cct.apply_no
),
con_d as (
  select
  cct.apply_no
  , SUM(cct.trans_money) tuifei_amt -- 退费
  from ods.ods_bpms_c_cost_trade cct
  where cct.trans_type = "CSD1" and cct.trade_status = 1
  group by cct.apply_no
)
insert overwrite table dws.tmp_archive_order
select
bao.apply_no,
bao.product_id,
bao.product_type,
bao.product_name,
bao.city_name,
bao.branch_id,
bao.branch_name,
con1.jiaofei_amt,
con1.premium_amt,
con_d1.tuifei_amt,
to_date(oco.financialarchiveevent_time) financialarchiveevent_time,
(case when product_type='保险类产品' then (nvl(con1.jiaofei_amt, 0) - nvl(con_d1.tuifei_amt, 0) )
when product_type='现金类产品' then  (nvl(con1.jiaofei_amt, 0) - nvl(con_d1.tuifei_amt, 0) )-ofm.cost_capital else 0 END )  revenue,
bao.release_amount,
bao.isjms
from  ods.ods_bpms_biz_apply_order_common bao
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx oco on oco.apply_no=bao.apply_no
LEFT JOIN ods.ods_bpms_biz_isr_mixed_common bim on bim.apply_no=bao.apply_no
left join (select * from con_t tip  )  con1 on con1.apply_no=bao.apply_no
left join (select * from con_d tip  )  con_d1 on con_d1.apply_no=bao.apply_no
left join ods.ods_orders_finance_common ofm on ofm.apply_no=bao.apply_no
where to_date(oco.financialarchiveevent_time)>='${hivevar:logdate}';

-- select * from  tmp_archive_order where financialarchiveevent_time is not null and financialarchiveevent_time!=''
-- create table dws.dws_stats_archive_order as
insert overwrite table dws.stats_archive_order
select
  dwd.id dimension_date_id, -- 年月
  dp.id dimension_product_id,  -- 产品类型,交易类型
  dso.s_key dim_sys_org_id, -- 分公司
  count(*) ordernum, -- 归档订单量
  sum(revenue), -- 净营收
  sum(revenue)/count(*) avg_revenue, -- 单均净营收
  sum(release_amount) release_amount, -- 放款金额
   '${hivevar:logdate}'
from (select * from tmp_archive_order where financialarchiveevent_time is not null and financialarchiveevent_time!='') tao
left join dws.dimension_date dwd on dwd.calendar=tao.financialarchiveevent_time
left join dim.dim_company dso on dso.company_id_3_level=tao.branch_id
left join dws.dimension_product dp on dp.product_code=tao.product_id
group by dwd.id,dp.id,dso.s_key