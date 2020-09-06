set hive.execution.engine=spark;
drop table if exists dws.tmp_report_revenue_part_two_detail_add;
create table if not exists dws.tmp_report_revenue_part_two_detail_add (
  `create_date` timestamp ,
  `apply_no` string ,
  `company_name` string ,
  `product_name` string ,
  `product_type` string ,
  `product_name_c` string ,
  `is_overtime` string  COMMENT '是否超期',
  `zt_amount` double  COMMENT '在途金额',
  `fk_amount` double  COMMENT '放款金额',
  `net_charge_amount` double  COMMENT '净收费',
  `customer_capital_cost` double COMMENT '客户资金成本',
  `lc_value` double  COMMENT '当日利差',
  `ljjsryg_amount` double  COMMENT '当日累计净收入预估',
  `drlrfy_amount` double COMMENT '当日录入返佣金额',
  `drzjcb_amount` double  COMMENT '当日资金成本',
  etl_update_time timestamp 
);

with tmp_biz_fee_detial as (

   SELECT 
   bfd.apply_no
   ,max(nvl(case when bfd.fee_define_no like '%baseFee%' then bfd.fee_value end, 0)) baseFee_value
   ,max(nvl(case when bfd.fee_define_no = 'overdueRatePerDay' then bfd.fee_value end, 0)/100) overdueRatePerDay
   ,max(nvl(case when bfd.fee_name like '%合同费率合计%' then bfd.fee_value end, 0)/100) htflhj_value
   ,max(nvl(case when bfd.fee_name like '%渠道费率合计%' then bfd.fee_value end, 0)/100) qdflhj_value
   from ods.ods_bpms_biz_fee_detial  bfd
   group by bfd.apply_no
),

tmp_t as(
  select 
  date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
  ,a.apply_no
  ,a.company_name
  ,a.product_name
  ,a.product_type
  ,a.product_name_c
  ,a.is_overtime
  ,sum(case when a.khhk_date is null or a.khhk_date> from_unixtime(unix_timestamp(),'yyyy-MM-dd') then nvl(a.fk_amount, 0)
     else 0
       end
   ) zt_amount

  from
  dws.tmp_report_revenue_part_one as a 
  where a.product_type in ('保险类产品产品', '现金类产品')
  and a.create_date <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) or (a.charge_trans_date <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.create_date is null)
  GROUP BY a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, a.is_overtime
),

tmp_fd_advance as (

  select 
  fd.apply_no
  ,max(fd.fin_date) max_fin_date
  from ods.ods_bpms_fd_advance fd 
  group by fd.apply_no 
),

tmp_slq as (
  SELECT
  t1.`apply_no`,
  if(max(t2.`fin_date`) is not null, '是', '否') AS advance_before_shulou,  -- 赎楼前垫资是否归还
  sum(t1.`adv_amt`)  AS  floorAdvance_adv_amt, -- 赎楼前垫资金额
  min(t1.`fin_date`)  AS  floorAdvance_fin_date, -- 赎楼前垫资放款时间
  max(t2.`fin_date`) AS  floorAdvance_ret_date -- 赎楼前垫资归还时间
  FROM ods.`ods_bpms_fd_advance` t1
  LEFT JOIN ods.`ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='floorAdvance' and t1.status <> "ZZ"
  group by t1.`apply_no`
),

tmp_dqgh as (
  SELECT
  t1.`apply_no`,
  if(max(t2.`fin_date`) is not null, '是', '否') AS advance_return, --到期归还垫资是否归还
  sum(t1.`adv_amt`)  AS  expireAdvance_adv_amt, -- 到期归还垫资金额
  min(t1.`fin_date`)  AS expireAdvance_fin_date, -- 到期归还垫资时间
  max(t2.`fin_date`)  AS expireAdvance_ret_date -- 到期归还垫资归还时间
  FROM ods.`ods_bpms_fd_advance` t1
  LEFT JOIN ods.`ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='expireAdvance' and  t1.status <> "ZZ"
  group by t1.`apply_no`
),

tmp_amt as (
	select
	bao.apply_no
	,bfs.borrowing_amount realease_amount -- 借款金额
	,bfs.borrowing_value_date -- 客户起息日
	,bfs.platform_value_date -- 平台起息日
	,if(bao.service_type = "JMS", "是", "否") is_jms -- 是否加盟商业务
	from ods.ods_bpms_biz_apply_order bao
	left join ods.ods_bpms_biz_isr_mixed bim on bao.apply_no = bim.apply_no
	left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
),

tmp_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0))  ftp
	,nvl(cfcr.pre_rate, 0) + nvl(cfcr.pre_amount/tmp_amt.realease_amount, 0)  pre_rate
	,nvl(cfcr.pre_rate, 0) ftp_pre_rate  -- 前置费率
	,nvl(cfcr.pre_amount, 0) ftp_pre_amount -- 前置金额
	from ods.ods_bpms_biz_apply_order a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	left join dws.tmp_report_revenue_part_one trr on a.apply_no = trr.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = trim(a.partner_insurance_name) -- 合作机构/垫资类型
		and cfcr.ftp_type = "营收"
	where trr.create_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and trr.create_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_slq_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0)) ftp
	from ods.ods_bpms_biz_apply_order a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_slq on tmp_slq.apply_no = a.apply_no
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = "赎楼前垫资" -- 合作机构/垫资类型
		and cfcr.ftp_type = "营收"
	where tmp_slq.floorAdvance_fin_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and tmp_slq.floorAdvance_fin_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_dqgh_ftp as (
	select
	a.apply_no
	,(nvl(cfcr.ftp, 0))  ftp
	from ods.ods_bpms_biz_apply_order a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join tmp_dqgh on tmp_dqgh.apply_no = a.apply_no
	left join tmp_amt on a.apply_no = tmp_amt.apply_no
	join ods.ods_bpms_c_fund_cost_rule cfcr
	     on cfcr.city_name = b.name_
		and cfcr.biz_type = if(tmp_amt.is_jms = "是", "加盟业务", "直营业务")
		and cfcr.institution = "到期归还垫资" -- 合作机构/垫资类型
		and cfcr.ftp_type = "营收"
	where tmp_dqgh.expireAdvance_fin_date >= to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.begin_time, 'yyyy/M/d'))) and tmp_dqgh.expireAdvance_fin_date < to_date(from_unixtime(UNIX_TIMESTAMP(cfcr.end_time, 'yyyy/M/d')))
),

tmp_fd_advance_expireAdvance as (

	select
	fd.apply_no 
	,max(fd.fin_date) max_fin_date
	from ods.ods_bpms_fd_advance fd 
    where fd.adv_type = 'expireAdvance'
    group by fd.apply_no
),

tmp_csc1 as (

    select
    apply_no, to_date(trans_day) trans_day_d, sum(trans_money) trans_money
    from ods.ods_bpms_c_cost_trade cct
    where cct.trans_type = 'CSC1'
    group by apply_no, to_date(trans_day)
),

tmp_csd1 as (

    select
    apply_no, to_date(trans_day) trans_day_d, sum(trans_money) trans_money
    from ods.ods_bpms_c_cost_trade cct
    where cct.trans_type = 'CSD1'   and cct.trade_status = '1'
    group by apply_no, to_date(trans_day)
),

tmp_charge_amount as (
    select
        a.apply_no
        ,a.company_name
        ,a.product_name
        ,a.product_type
        ,a.product_name_c
        ,a.create_date
        ,a.is_overtime
        ,sum(a.charge_amount) - sum(a.return_premium_amount)  net_charge_amount
       from
        (select
        a.apply_no
        ,a.company_name
        ,a.product_name
        ,a.product_type
        ,a.product_name_c
        ,b.trans_day_d create_date
        ,a.is_overtime
        ,nvl(sum(b.trans_money), 0) charge_amount
        , 0 return_premium_amount
        from tmp_csc1 as b
        left join dws.tmp_report_revenue_part_one as a on a.apply_no = b.apply_no

        where b.trans_day_d = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)
        GROUP BY a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, b.trans_day_d, a.is_overtime

        union all

        select
        a.apply_no
        ,a.company_name
        ,a.product_name
        ,a.product_type
        ,a.product_name_c
        ,b.trans_day_d create_date
        ,a.is_overtime
        ,0 charge_amount
        ,nvl(sum(b.trans_money), 0) return_premium_amount
        from tmp_csd1 as b
        left join dws.tmp_report_revenue_part_one as a on a.apply_no = b.apply_no
        where b.trans_day_d = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)
        GROUP BY a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, b.trans_day_d, a.is_overtime

    ) as a
    group by a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, a.create_date, a.is_overtime
),

tmp_lrfy as (
    select a.apply_no
      ,a.company_name
      , a.product_name
      , a.product_type
      , a.product_name_c
      , a.is_overtime
      , a.create_date
      , nvl(sum(a.hsfy_amount), 0) + nvl(sum(a.fpfy_amount), 0) + nvl(sum(a.dsdffy_amount), 0) drlrfy_amount
    from(
            --  当日录入返佣金额-汇思支付信
            select sum(s_bl.humanpool_rebate) hsfy_amount
            , 0 fpfy_amount
            , 0 dsdffy_amount
            , date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
            , irpo.apply_no
            , irpo.company_name
            , irpo.product_name
            , irpo.product_type
            , irpo.product_name_c
            , irpo.is_overtime
            from ods.ods_bpms_biz_ledger_pay s_bl
            join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
            where s_bl.humanpool_payment_day >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and s_bl.humanpool_payment_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
            group by irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime, irpo.apply_no


            union all
            --  当日录入返佣金额-发票报销

            select
            0 hsfy_amount
            ,sum(s_bl.invoice_rebate) fpfy_amount
            ,0 dsdffy_amount
            , date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
            , irpo.apply_no
            , irpo.company_name
            , irpo.product_name
            , irpo.product_type
            , irpo.product_name_c
            , irpo.is_overtime
            from ods.ods_bpms_biz_ledger_invoice s_bl
            join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
            where s_bl.invoice_submit_day >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and s_bl.invoice_submit_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
            group by irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime, irpo.apply_no

             union all
            --  当日录入返佣金额-代收代付返佣

            select
            0 hsfy_amount
            ,0 fpfy_amount
            ,sum(replace_turn_out) dsdffy_amount
            , date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
            , irpo.apply_no
            , irpo.company_name
            , irpo.product_name
            , irpo.product_type
            , irpo.product_name_c
            , irpo.is_overtime
            from ods.ods_bpms_biz_ledger_instead s_bl
            join dws.tmp_report_revenue_part_one irpo on s_bl.apply_no = irpo.apply_no
            where s_bl.replace_turn_day >= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and s_bl.replace_turn_day < date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)
            group by irpo.company_name, irpo.product_name, irpo.product_type, irpo.product_name_c, irpo.is_overtime, irpo.apply_no

        )as a
        group by a.company_name, a.product_name, a.product_type, a.product_name_c, a.is_overtime, a.create_date, a.apply_no
)

insert overwrite table dws.tmp_report_revenue_part_two_detail_add
select 
a.create_date, a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, a.is_overtime
,cast(sum(zt_amount) as double) zt_amount --  在途金额
,cast(sum(fk_amount) as double) fk_amount --  放款金额
,cast(sum(net_charge_amount) as double) net_charge_amount --  净收费
,cast(sum(customer_capital_cost) as double) customer_capital_cost --  当日客户资金成本
,cast(sum(lc_value) as double) lc_value
,cast(case when a.product_type = "保险类产品" or a.product_type = "服务类产品"  then round(sum(net_charge_amount), 6)
	 else round(sum(ljjsryg_amount) , 6)
	 end as double) ljjsryg_amount
,cast(sum(drlrfy_amount) as double) drlrfy_amount --  当日录入返佣金额
,cast(sum(drzjcb_amount) as double) drzjcb_amount --  当日资金成本
,from_unixtime(unix_timestamp(),'yyyy-MM-dd') etl_update_time
from 
    (select
    a.create_date
    ,a.apply_no
    ,a.company_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,a.is_overtime
    ,nvl(a.zt_amount, 0) zt_amount --  在途金额
    ,nvl(b.fk_amount,0) fk_amount --  放款金额
    ,0 net_charge_amount --  净收费
    ,nvl(a.customer_capital_cost_p1, 0)/nvl(a.zt_amount, 0)*360 customer_capital_cost --  当日客户资金成本
    ,round(nvl(a.lc_amount_part_1, 0) - nvl(a.lc_amount_part_2, 0),6) lc_value
    ,round(case  when a.product_type = "现金类产品"
                 then (round(nvl(a.lc_amount_part_1, 0) - nvl(a.lc_amount_part_2, 0),6)) * nvl(a.zt_amount, 0) / 360
           else 0
           end, 3) ljjsryg_amount
    ,0 drlrfy_amount --  当日录入返佣金额
    ,a.drzjcb_amount drzjcb_amount --  当日资金成本
    from (
            select
            date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) create_date
            ,a.apply_no
            ,a.company_name
            ,a.product_name
            ,a.product_type
            ,a.product_name_c
            ,a.is_overtime
            ,max(zta.zt_amount) zt_amount -- 每个都一样
            ,sum(case
                  -- /* ∑（基础收费/预估用款期限*1天）*/
                 when a.product_term_and_charge_way = "固定期限" and a.is_overtime = "正常" and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then (tbfd.baseFee_value / a.fixed_term)

                  -- /* ∑超期费率*借款金额 */
                 when a.product_term_and_charge_way = "固定期限" and a.is_overtime = "超期" and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then (nvl(a.fk_amount, 0) * tbfd.overdueRatePerDay)

                  -- /*
                  --  [∑（借款金额*合同费率合计/预估用款期限）]
                  -- */
                 when a.product_term_and_charge_way = "按天计息"  and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then nvl(a.fk_amount, 0) * tbfd.htflhj_value / a.fixed_term


                 when a.product_term_and_charge_way = '固定费率' and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then

                      case

                       -- /* 统计日非回款日：[∑（合同费率合计/预估用款期限*借款金额）] */
                        when a.fixed_term > a.capital_use_time and a.khhk_date <> a.create_date
                        then nvl(a.fk_amount, 0) * tbfd.htflhj_value / a.fixed_term

                        -- /* 统计日为回款日：[∑（合同费率合计/预估用款期限*借款金额）*(预估用款期限-实际用款天数)] */
                        when a.fixed_term > a.capital_use_time and a.khhk_date = a.create_date
                        then nvl(a.fk_amount, 0) * tbfd.htflhj_value / a.fixed_term * (a.fixed_term - a.capital_use_time)

                      end


                   -- /* 统计日非回款日：[∑（CN/XN对应的天数*借款金额）] */
                 when a.product_term_and_charge_way = '区间计息' and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)) and a.is_overtime = '正常'
                 then
                      case
                        when a.capital_use_time <= a.firstPartOfChargingTerm
                        then a.channelPricePerOrderFirstPart / 100 /a.firstPartOfChargingTerm * nvl(a.fk_amount, 0)

                        when a.capital_use_time > a.firstPartOfChargingTerm
                        then a.channelPricePerOrderPerPart / 100 /a.chargingTermGap * nvl(a.fk_amount, 0)
                      end

                    -- /* 统计日为回款日：[∑（CN/XN对应的天数*借款金额）*XN剩余天数] */
                 when a.product_term_and_charge_way = '区间计息' and a.is_overtime = '正常'
                 then
                        case  when a.khhk_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.capital_use_time <= a.firstPartOfChargingTerm
                              then a.channelPricePerOrderFirstPart / 100 /a.firstPartOfChargingTerm * nvl(a.fk_amount, 0) * (a.firstPartOfChargingTerm-a.capital_use_time+1)

                              when a.khhk_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.capital_use_time > a.firstPartOfChargingTerm
                              then a.channelPricePerOrderPerPart / 100 /a.chargingTermGap * nvl(a.fk_amount, 0)
                                   *(a.chargingTermGap-pmod((a.capital_use_time - a.firstPartOfChargingTerm), a.chargingTermGap) +1)
                        end

                        -- /*∑超期费率*借款金额]*/
                 when a.product_term_and_charge_way = '区间计息' and a.is_overtime = '超期' and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then (nvl(a.fk_amount, 0) * tbfd.overdueRatePerDay)

                 else 0
                 end
            ) customer_capital_cost_p1

            ,sum(case
                 -- /* [∑（借款金额*渠道费率合计/预估用款期限）]/当日在途余额*360 */
                 when a.product_term_and_charge_way = "按天计息"  and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then nvl(a.fk_amount, 0) * tbfd.qdflhj_value / a.fixed_term

                 when a.product_term_and_charge_way = "固定期限" and a.is_overtime = "正常"  and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then nvl(a.fk_amount, 0) * tbfd.qdflhj_value / a.fixed_term

                 when a.product_term_and_charge_way = "固定期限" and a.is_overtime = "超期"  and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then (nvl(a.fk_amount, 0) * tbfd.overdueRatePerDay)

                 when a.product_term_and_charge_way = "固定费率" and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                 then
                        case

                        -- /* 统计日非回款日：[∑（渠道费率合计/预估用款期限*借款金额）] */
                        when a.fixed_term > a.capital_use_time and a.khhk_date <> a.create_date
                        then nvl(a.fk_amount, 0) * tbfd.qdflhj_value / a.fixed_term


                        -- /* 统计日为回款日：[∑（渠道费率合计/预估用款期限*借款金额）*(预估用款期限-实际用款天数)] */
                        when a.fixed_term > a.capital_use_time and a.khhk_date = a.create_date
                        then nvl(a.fk_amount, 0) * tbfd.qdflhj_value / a.fixed_term * (a.fixed_term - a.capital_use_time)


                              end

                 when a.product_term_and_charge_way = '区间计息' and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)) and a.is_overtime = '超期'
                 then (nvl(a.fk_amount, 0) * tbfd.overdueRatePerDay)

                       -- /* 统计日非回款日：[∑（CN/XN对应的天数*借款金额）] */
                 when a.product_term_and_charge_way = '区间计息' and (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)) and a.is_overtime = '正常'
                 then
                        case
                        when a.capital_use_time <= a.firstPartOfChargingTerm
                        then a.channelPricePerOrderFirstPart/100/a.firstPartOfChargingTerm*nvl(a.fk_amount, 0)

                        when a.capital_use_time > a.firstPartOfChargingTerm
                        then a.channelPricePerOrderPerPart/100/a.chargingTermGap*nvl(a.fk_amount, 0)
                        end

                        -- /* 统计日为回款日：[∑（CN/XN对应的天数*借款金额）*XN剩余天数] */
                 when a.product_term_and_charge_way = '区间计息' and a.is_overtime = '正常'
                 then
                        case
                        when a.khhk_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.capital_use_time <= a.firstPartOfChargingTerm
                        then a.channelPricePerOrderFirstPart/100/a.firstPartOfChargingTerm*nvl(a.fk_amount, 0)*(a.firstPartOfChargingTerm-a.capital_use_time+1)

                        when a.khhk_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.capital_use_time > a.firstPartOfChargingTerm
                        then a.channelPricePerOrderPerPart/100/a.chargingTermGap*nvl(a.fk_amount, 0)*(a.chargingTermGap-pmod((a.capital_use_time - a.firstPartOfChargingTerm), a.chargingTermGap) +1)
                        end


                 end)/max(zta.zt_amount)*360 lc_amount_part_1

            ,nvl(
                sum(
                  (
                    nvl(a.fk_amount, 0)*
                    if((a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)),
                        (case when  tfa.max_fin_date is not null
                                    and bfs.platform_value_date is null
                                    and (a.khhk_date is null or a.khhk_date > date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                              then tmp_slq_ftp.ftp / 360

                              when bfs.platform_value_date is not null
                                   and tfae.max_fin_date is null

                              then tmp_ftp.ftp / 360

                              when bfs.platform_value_date is not null
                                   and tfae.max_fin_date is not null

                              then tmp_dqgh_ftp.ftp / 360
                        end)

                    ,0)
                    + nvl(nvl(a.fk_amount, 0)*tmp_ftp.pre_rate, 0)
                  )/zta.zt_amount
                )
            ,0) lc_amount_part_2

            ,sum(
                if(
                    (a.khhk_date is null or a.khhk_date> date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1)),
                    nvl(a.fk_amount, 0)*
                    (case when  tfa.max_fin_date is not null and (bfs.platform_value_date is null or bfs.platform_value_date = "")
                                and (a.khhk_date is null or a.khhk_date > date_add(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1), 1))
                          then tmp_slq_ftp.ftp / 360

                          when bfs.platform_value_date is not null
                               and tfae.max_fin_date is null

                          then tmp_ftp.ftp / 360

                          when bfs.platform_value_date is not null
                               and tfae.max_fin_date is not null

                          then tmp_dqgh_ftp.ftp / 360
                    end)

                  + nvl(nvl(a.fk_amount, 0)*tmp_ftp.pre_rate, 0)
                , 0)
            ) drzjcb_amount
            from
            dws.tmp_report_revenue_part_one as a
            left join ods.ods_bpms_biz_fee_summary bfs on a.apply_no = bfs.apply_no
            left join tmp_biz_fee_detial tbfd on a.apply_no = tbfd.apply_no
            left join tmp_fd_advance tfa on a.apply_no = tfa.apply_no
            left join tmp_slq_ftp on a.apply_no = tmp_slq_ftp.apply_no
            left join tmp_ftp on a.apply_no = tmp_ftp.apply_no
            left join tmp_dqgh_ftp on a.apply_no = tmp_dqgh_ftp.apply_no
            left join tmp_fd_advance_expireAdvance tfae on a.apply_no = tfae.apply_no
            left join tmp_t as zta on a.company_name = zta.company_name
                    and a.product_name = zta.product_name
                    and a.product_type = zta.product_type
                    and a.product_name_c = zta.product_name_c
                    and a.is_overtime = zta.is_overtime
                    and a.apply_no = zta.apply_no

            where a.create_date <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) or (a.charge_trans_date <= date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) and a.create_date is null)
            GROUP BY a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, a.create_date, a.is_overtime
    ) as a

    left join (
        select
        a.apply_no
        ,a.company_name
        ,a.product_name
        ,a.product_type
        ,a.product_name_c
        ,a.create_date
        ,a.is_overtime
        ,nvl(sum(a.fk_amount), 0) fk_amount
        from
        dws.tmp_report_revenue_part_one as a
        where a.create_date = date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)
        GROUP BY a.apply_no, a.company_name, a.product_name, a.product_type, a.product_name_c, a.create_date, a.is_overtime
    ) as b on a.company_name = b.company_name
            and a.product_name = b.product_name
            and a.product_type = b.product_type
            and a.product_name_c = b.product_name_c
            and a.create_date = b.create_date
            and a.is_overtime = b.is_overtime
            and a.apply_no = b.apply_no

    where a.zt_amount > 0 or a.customer_capital_cost_p1 > 0 or a.lc_amount_part_1 > 0 or a.lc_amount_part_2 > 0 or a.drzjcb_amount > 0 or b.fk_amount > 0

    union all

     select a.create_date
      ,a.apply_no
      ,a.company_name
      ,a.product_name
      ,a.product_type
      ,a.product_name_c
      ,a.is_overtime
      ,0 zt_amount --  在途金额
      ,0 fk_amount --  放款金额
      ,a.net_charge_amount --  净收费
      ,0 customer_capital_cost --  当日客户资金成本
      ,0 lc_value
      ,0 ljjsryg_amount
      ,0 drlrfy_amount --  当日录入返佣金额
      ,0 drzjcb_amount --  当日资金成本
     from tmp_charge_amount as a

     union all

    select
    a.create_date
    ,a.apply_no
    ,a.company_name
    ,a.product_name
    ,a.product_type
    ,a.product_name_c
    ,a.is_overtime
    ,0 zt_amount --  在途金额
    ,0 fk_amount --  放款金额
    ,0 net_charge_amount --  净收费
    ,0 customer_capital_cost --  当日客户资金成本
    ,0 lc_value
    ,0 ljjsryg_amount
    ,a.drlrfy_amount --  当日录入返佣金额
    ,0 drzjcb_amount --  当日资金成本
    from tmp_lrfy as a


) as a
group by a.company_name, a.product_name, a.product_type, a.product_name_c, a.is_overtime, a.create_date, a.apply_no

;
