use ods ;
drop table if exists dwd.tmptmp_pt_product;
create table if not exists dwd.tmptmp_pt_product (
  apply_no STRING COMMENT '订单编号',
  auxiliary_apply_no STRING COMMENT '配套订单号',
  auxiliary_product_name STRING COMMENT '配套产品'
)STORED AS PARQUET;
with tmp_seel as (
select a.apply_no,
group_concat(a.name) seller_name, -- 卖方：SEL
group_concat(a.phone) seller_phone, --手机
group_concat(a.id_card_no) seller_id_card_no, -- 身份证
group_concat(employer) employer, --工作单位
group_concat(income_type_name) income_type_name, -- 收入类型
group_concat(marital_status_tag) marital_status_name -- 婚姻状态
from (select * from ods_bpms_biz_customer_rel_common order by is_actual_borrower_name desc) a where  a.`role`='OWN' and (a.relation_name='产权人' or a.relation_name='产权人配偶'
or a.relation_name='新贷款借款人' or a.relation_name='卖方' or (a.is_actual_borrower_name='Y' and a.relation_name='原贷款借款人'))
group by a.apply_no
),
tmp_buy as (
select a.apply_no,
group_concat(a.name) buy_name, -- 买方：BUY
group_concat(a.phone) buy_phone,
group_concat(a.id_card_no) buy_id_card_no,
group_concat(a.income_type_name) income_type_name,  -- 收入类型
group_concat( employer) employer, -- 工作单位
group_concat(marital_status_tag) marital_status_name  -- 婚姻状态
from ods_bpms_biz_customer_rel_common a where  a.`role`='BUY' and a.`relation`='BUY' group by a.apply_no
),
tmp_all_customer as (
  select
  ts.apply_no
  ,bao.product_name
  ,bp.sfjy
  ,ts.seller_id_card_no
  ,tb.buy_id_card_no
  -- ,case when ts.seller_id_card_no = "440304199003052215" then ts.apply_no else ts.seller_id_card_no end seller_id_card_no
  -- ,case when ts.seller_id_card_no = "440304199003052215" then ts.apply_no else tb.buy_id_card_no end  buy_id_card_no
  ,bao.apply_status_name
  from tmp_seel ts
    join tmp_buy tb on ts.apply_no = tb.apply_no
    join ods.ods_bpms_biz_apply_order_common bao on ts.apply_no = bao.apply_no
    join ods.ods_bpms_b_product bp on bao.product_name = bp.product_name
)
insert overwrite table dwd.`tmptmp_pt_product`
select
bao.apply_no
,group_concat(tac_s.apply_no) auxiliary_apply_no  -- 配套订单号
,group_concat(tac_s.product_name) auxiliary_product_name -- 配套产品
from ods.ods_bpms_biz_apply_order_common bao
left join tmp_all_customer tac on bao.apply_no = tac.apply_no
left join (select apply_no,apply_status_name,product_name,sfjy,buy_id_card_no,seller_id_card_no from tmp_all_customer where seller_id_card_no is not null and buy_id_card_no is not null) tac_s on tac.seller_id_card_no = tac_s.seller_id_card_no  and tac.buy_id_card_no = tac_s.buy_id_card_no
where tac.apply_no <> tac_s.apply_no     -- 交易类
and tac_s.sfjy = '1'  and tac_s.product_name <> "大道按揭" and tac_s.apply_status_name not in ("已终止", "拒绝")
group by bao.apply_no;
drop table if exists dwd.tmp_pt_product;
ALTER TABLE dwd.tmptmp_pt_product RENAME TO dwd.tmp_pt_product;