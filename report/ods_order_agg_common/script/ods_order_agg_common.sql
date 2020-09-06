set hive.execution.engine=spark;
drop table if exists ods.odstmporder_agg_common;
create table if not exists ods.odstmporder_agg_common (
  apply_no string comment '业务编号'
  ,product_name string comment '产品名称'
  ,apply_time timestamp comment '订单申请时间'
  ,interview_num_xg double comment '面签笔数_销管'
  ,manual_end_time timestamp comment '订单终止时间'
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

insert overwrite table ods.odstmporder_agg_common
select 
bao.apply_no 
,bao.product_name
,bao.apply_time
,(case when t666.interview_time_min is null then 0
    else
     (case
        when bao.product_name='保险服务'
           or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
           or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
           then 0
        when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
        when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and nvl(obbh.fcsl, 0)=0)
           then 1  -- 和放款笔数折算不同
        when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
           then 0.5
        else 0
     end )
end ) interview_num_xg  -- 面签笔数 销管口径
,tme.complete_time_ manual_end_time   -- 订单终止时间
from ods.ods_bpms_biz_apply_order_common bao
left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
left join ods.ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join tmp_bco bco on bco.apply_no=bao.apply_no
left join ods.ods_bpms_sys_dic sd_pt on (bfs.`price_tag`=sd_pt.KEY_ and sd_pt.`TYPE_ID_`='10000042640043')
left join tmp_obbh obbh on obbh.house_no = bao.house_no
left join tmp_manual_end tme on bao.flow_instance_id = tme.PROC_INST_ID_
;

drop table if exists ods.ods_order_agg_common;
ALTER TABLE ods.odstmporder_agg_common RENAME TO ods.ods_order_agg_common;