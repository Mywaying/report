set hive.execution.engine=spark;
drop table if exists ods.odstmpfinance_agg_common;
create table if not exists ods.odstmpfinance_agg_common (
  apply_no string comment '业务编号'
  ,product_name string comment '产品名称'
  ,apply_time timestamp comment '订单申请时间'
  ,loan_time_xg timestamp comment '放款时间_销管'
  ,loan_num_xg double comment '放款笔数_销管'
  ,release_amount_xg double comment '放款金额_销管'
  ,return_status string comment '回款状态'
);

with tmp_bco as (
	select 
	apply_no
	,concat_ws(',', collect_set(OPINION_)) OPINION_
	from ods.ods_bpms_bpm_check_opinion_common 
	where `STATUS_`='manual_end' 
	group by apply_no
),

tmp_obbh as (
	select 
	bh.house_no
	, count(*) fcsl 
	from ods.ods_bpms_biz_house bh
	where bh.house_type not in ("qt", "cw") and bh.house_no is not null and bh.house_no <> "" and bh.in_reserve_house <> "Y"
	group by bh.house_no 
),

tmp_cfm as (
	select b.* 
	from( 
		select 
		a.* 
		,ROW_NUMBER() OVER(PARTITION BY a.apply_no ORDER BY create_time desc) rank
		from ods.ods_bpms_c_fund_module_common a
	) b
	where rank = 1
),

tmp_manual_end as (
	select a.* 
	from(
		SELECT 
		PROC_INST_ID_, complete_time_
		,ROW_NUMBER() OVER(PARTITION BY PROC_INST_ID_ ORDER BY complete_time_ desc) rank
		from ods.ods_bpms_bpm_check_opinion
		where status_ in ('manual_end','sys_auto_end')
	) a  
	where rank = 1
),

cct_cc01 as (
  select 
  cct.apply_no
  ,SUM(cct.`trans_money`) receive_amount_hk
  from ods.ods_bpms_c_cost_trade cct 
  where cct.trans_type = "CC01" 
  group by cct.apply_no
)

insert overwrite table ods.odstmpfinance_agg_common
select 
bao.apply_no 
,bao.product_name
,bao.apply_time
,oofc.loan_time_xg -- 放款时间_销管
,(case when oofc.loan_time_xg is null then 0
    else
      (case
            when bao.product_name='保险服务'
               or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
               or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
               then 0
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
            when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl=0)
               then 1  -- 和放款笔数折算不同
            when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
               then 0.5
            else 0
         end )
 end )  loan_num_xg -- 放款笔数_销管
,(case
      when oofc.loan_time_xg is null then 0
      when bao.product_name in ('大道按揭', '保险服务') or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%')) or (bao.product_type='现金类产品' and bao.relate_type_name = '到期更换平台') then 0
      when bao.product_type='现金类产品' then cfm.con_funds_cost
      when bao.product_name like'买付保%' then bfs.guarantee_amount
      else nvl(bnl.biz_loan_amount, 0) 
 end ) release_amount_xg  -- 放款金额_销管
,case when bao.product_type = "现金类产品" and oofc.loan_time_xg is not null and cct_cc01.receive_amount_hk is null
      then "未回款"
      when bao.product_type = "现金类产品" and cct_cc01.receive_amount_hk > 0 
           and cct_cc01.receive_amount_hk < 
              (case when bao.product_type='现金类产品' then bfs.borrowing_amount
               when bao.product_type='保险类产品' then nvl(bfs.ransom_borrow_amount, brf_z.ransom_borrow_amount)
               else 0 
               end)
      then "回款中"
      when bao.product_type = "现金类产品" 
           and cct_cc01.receive_amount_hk >= 
              (case when bao.product_type='现金类产品' then bfs.borrowing_amount
               when bao.product_type='保险类产品' then nvl(bfs.ransom_borrow_amount, brf_z.ransom_borrow_amount)
               else 0
               end)
      then "回款完结"
 end return_status -- 回款状态
from ods.ods_bpms_biz_apply_order_common bao
left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join tmp_bco bco on bco.apply_no=bao.apply_no
left join ods.ods_bpms_sys_dic sd_pt on (bfs.`price_tag`=sd_pt.KEY_ and sd_pt.`TYPE_ID_`='10000042640043')
left join tmp_obbh obbh on obbh.house_no = bao.house_no
left join ods.ods_orders_finance_common oofc on oofc.apply_no = bao.apply_no
left join tmp_cfm cfm on cfm.apply_no=bao.apply_no
LEFT JOIN (select * from ods.ods_bpms_biz_new_loan_common where rn=1) bnl on bnl.apply_no=bao.apply_no
left join tmp_manual_end tme on bao.flow_instance_id = tme.PROC_INST_ID_
left join (select * from ods.ods_bpms_biz_ransom_floor_common where rn=1 ) brf_z on bao.apply_no = brf_z.apply_no
left join cct_cc01 on cct_cc01.apply_no = bao.apply_no
;

drop table if exists ods.ods_finance_agg_common;
ALTER TABLE ods.odstmpfinance_agg_common RENAME TO ods.ods_finance_agg_common;