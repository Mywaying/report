set hive.execution.engine=spark;
drop table if exists dws.tmp_report_revenue_part_one;
create table if not exists dws.tmp_report_revenue_part_one (
  apply_no string ,
  partner_insurance_name string ,
  company_name string COMMENT '公司名称',
  sctd_name string COMMENT '市场团队',
  is_jms string COMMENT '是否加盟商',
  qdjl_name string COMMENT '渠道经理',
  product_name string COMMENT '产品名称',
  product_type string COMMENT '产品类型',
  is_overtime string COMMENT '是否超期',
  capital_use_time bigint comment '实际用款期限',
  fixed_term bigint comment '固定期限',
  product_name_c string,
  product_term_and_charge_way string COMMENT '计费类型',
  create_date timestamp COMMENT '日期',
  fk_amount double COMMENT '放款金额',
  charge_trans_date timestamp ,
  charge_amount double COMMENT '收费金额',
  return_premium_date timestamp  COMMENT '退费日期',
  return_premium_amount double COMMENT '退费金额',
  order_flag bigint ,
  khhk_date timestamp  COMMENT '客户还款时间',
  firstPartOfChargingTerm double  COMMENT '区间计息-首段期限',
  chargingTermGap double  COMMENT '区间计息-期限间隔',
  channelPricePerOrderFirstPart double  COMMENT '区间计息-首段期限渠道价/笔(%)',
  channelPricePerOrderPerPart double  COMMENT '区间计息-其他时间段渠道价/段(%)'
);

with tmp_cc01 as (

  select
  cct.apply_no 
  ,max(trans_day) max_trans_day
  from ods.ods_bpms_c_cost_trade cct 
  where cct.trans_type = 'CC01'  
  group by cct.apply_no
),

tmp_csc1 as (

  select
  cct.apply_no 
  ,max(trans_day) max_trans_day
  ,sum(trans_money) trans_money
  from ods.ods_bpms_c_cost_trade cct 
  where cct.trans_type = 'CSC1'  
  group by cct.apply_no
),

tmp_csd1 as (

  select
  cct.apply_no 
  ,max(trans_day) max_trans_day
  ,sum(trans_money) trans_money
  from ods.ods_bpms_c_cost_trade cct 
  where cct.trans_type = 'CSD1' and cct.trade_status = '1'
  group by cct.apply_no
),

tmp_order_matter_record as (

  select
  bomr.apply_no 
  ,min(case when bomr.matter_key = 'ConfirmArrival' then bomr.create_time end) ConfirmArrival_time
  ,max(case when bomr.matter_key = 'RandomMark' then bomr.handle_time end) RandomMark_time
  ,max(case when bomr.matter_key = 'TransferMark' then bomr.handle_time end) TransferMark_time
  from ods.ods_bpms_biz_order_matter_record bomr 
  group by bomr.apply_no
),

tmp_biz_new_loan as (

  select 
  bnl.house_no
  ,max(biz_loan_amount) max_biz_loan_amount
  from ods.ods_bpms_biz_new_loan bnl 
  group by bnl.house_no
),

tmp_biz_base_fee_factor_value_rel as (
  select 
  bbffvr.fee_factor_id
  ,bbffvr.version
  ,max(case when bbfm.fee_metadata_code = 'firstPartOfChargingTerm' then bbffvr.value end) firstPartOfChargingTerm
  ,max(case when bbfm.fee_metadata_code = 'chargingTermGap' then bbffvr.value end) chargingTermGap
  ,max(case when bbfm.fee_metadata_code = 'channelPricePerOrderFirstPart' then bbffvr.value end) channelPricePerOrderFirstPart
  ,max(case when bbfm.fee_metadata_code = 'channelPricePerOrderPerPart' then bbffvr.value end) channelPricePerOrderPerPart
  from  ods.ods_bpms_biz_base_fee_factor_value_rel bbffvr 
  left join ods.ods_bpms_biz_base_fee_metadata bbfm on bbffvr.fee_metadata_id = bbfm.id
  group by bbffvr.fee_factor_id, bbffvr.version
),

tmp_qjjx_part_one as (
  SELECT 
  bbfftr.id
  ,bbfftr.start_time
  ,bbfftr.end_time
  ,bbfftr.version
  ,bbff.company_name city_name
  ,bbff.company_code city_code
  ,bbff.product_name
  ,bbff.product_code
  ,bbff.charge_way_Name
  ,bbff.charge_way_code
  ,bbff.channel_name
  ,bbff.franchisee_code
  ,bbff.franchisee_name
  ,tbbffvr.firstPartOfChargingTerm -- 首段期限
  ,tbbffvr.chargingTermGap -- 期限间隔
  ,tbbffvr.channelPricePerOrderFirstPart -- 首段期限渠道价/笔(%)
  ,tbbffvr.channelPricePerOrderPerPart -- 其他时间段渠道价/段(%)
  From ods.ods_bpms_biz_base_fee_factor_template_rel bbfftr
  left join ods.ods_bpms_biz_base_fee_factor bbff on bbfftr.fee_factor_id = bbff.id 
  left join tmp_biz_base_fee_factor_value_rel tbbffvr on bbfftr.fee_factor_id = tbbffvr.fee_factor_id and bbfftr.version = tbbffvr.version 
  where bbff.charge_way_code = 'piecewiseCalculate'
),

tmp_qjjx as (
  select 
   idof.apply_no
   ,iqr.firstPartOfChargingTerm
   ,iqr.chargingTermGap
   ,iqr.channelPricePerOrderFirstPart
   ,iqr.channelPricePerOrderPerPart
  from ods.ods_bpms_biz_apply_order_common idof
  left join ods.ods_bpms_s_area_info sai on idof.branch_id = sai.company_code and sai.remarks = '1'
  join tmp_qjjx_part_one iqr 
    on idof.product_id = iqr.product_code and sai.name = iqr.city_name 
  where idof.apply_time >= iqr.start_time and idof.apply_time < nvl(end_time, cast("2030-01-01" as timestamp))
)

insert overwrite table dws.tmp_report_revenue_part_one
select 
bao.apply_no
,bao.partner_insurance_name
,sai.NAME_ company_name --  '业务公司'
,so.NAME_ sctd_name --  市场团队
,bao.isjms --  是否加盟商
,su.fullname_  qdjl_name --  渠道经理
,case when bao.product_name = "提放保-无赎楼" then '提放保（无赎楼）'
      when bao.product_name = "提放保-有赎楼" then '提放保（有赎楼）'
      else bao.product_name
      end product_name

,bao.product_type

,case when bao.product_name like "%及时贷%"  and (bfs.product_term_and_charge_way='N' or  bfs.product_term_and_charge_way='fixedTerm' or bfs.product_term_and_charge_way = "piecewiseCalculate")
      and bao.product_name not like "%贷款服务%" 
      and nvl(datediff(nvl(tmp_cc01.max_trans_day, cast(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) as timestamp)), bfs.borrowing_value_date), 0) > bfs.fixed_term
      then "超期"

      when bao.product_name like "%及时贷%"  and  (bfs.product_term_and_charge_way='N' or  bfs.product_term_and_charge_way='fixedTerm' or bfs.product_term_and_charge_way = "piecewiseCalculate")
      and bao.product_name not like "%贷款服务%" 
      and nvl(datediff(nvl(tmp_cc01.max_trans_day, cast(date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1) as timestamp)), bfs.borrowing_value_date), 0) <= bfs.fixed_term
      then "正常"
     
      else ""
end is_overtime

,nvl(datediff(tmp_cc01.max_trans_day, bfs.borrowing_value_date), 0) capital_use_time

,bfs.fixed_term
,case when bao.product_name like "%交易保%" then '交易保'
     when bao.product_name like "%提放保%" then '提放保'
     when bao.product_name like "%拍卖保%" then '拍卖保'
     when bao.product_name like "%买付保%" then '买付保'
     when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%" and  (bfs.product_term_and_charge_way='Y' or bfs.product_term_and_charge_way='calculateDaily')
        then concat(bao.product_name, '-', '按天计息')
     when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%" and  (bfs.product_term_and_charge_way='N' or bfs.product_term_and_charge_way='fixedTerm')
        then concat(bao.product_name, '-', '固定期限')
     when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%" and  (bfs.product_term_and_charge_way='piecewiseCalculate')
        then concat(bao.product_name, '-', '区间计息')
     when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%" and  (bfs.product_term_and_charge_way='fixedRate')
        then concat(bao.product_name, '-', '固定费率')
     else bao.product_name
  end product_name_c

,case when bfs.product_term_and_charge_way='Y' or bfs.product_term_and_charge_way='calculateDaily' then '按天计息'
   when bfs.product_term_and_charge_way='N' or bfs.product_term_and_charge_way='fixedTerm' then '固定期限'
   when bfs.product_term_and_charge_way='piecewiseCalculate' then '区间计息'
   when bfs.product_term_and_charge_way = 'fixedRate' then '固定费率'
   else null
end product_term_and_charge_way

-- ----------------------- 时间-----------------------------------------------------------------
,to_date(
     case when (bao.product_name like "%交易保%" or bao.product_name like "%提放保%")
          then bim.bank_loan_time

          when (bao.product_name = '买付保（有赎楼）' and bfs.random_pay_mode = 'companyHelpPay')
             or bao.product_name = '买付保（无赎楼）'
             or bao.product_name = "买付保（有赎楼）（大道保障模式）"
          then tomr.ConfirmArrival_time

          when bao.product_name = '买付保（有赎楼）' and bfs.random_pay_mode = 'buyerPay' and brf.ransom_flag = 'Y'
             then tomr.RandomMark_time

          when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%"
             then bfs.borrowing_value_date

          when (bao.product_name = '及时贷（贷款服务）'
             or bao.product_name = '大道快贷（贷款服务）'
             or bao.product_name = '大道易贷（贷款服务）'
             or bao.product_name = '限时贷')
             or bao.product_name like "%拍卖保%"
          then tomr.TransferMark_time

         else null
     end
 ) create_date

-- ----------------------- 当日放款金额（万元）-----------------------------------------------------------------
,(case when (bao.product_name like "%交易保%" or bao.product_name like "%提放保%")
      then bim.bank_loan_amount 

      when (bao.product_name like '%买付保%')
      then bfs.guarantee_amount
    
      when bao.product_name like "%及时贷%" and bao.product_name not like "%贷款服务%" and  bfs.borrowing_value_date is not null 
      then bfs.borrowing_amount
      
      when ( bao.product_name = "及时贷（贷款服务）"
        or bao.product_name = "大道快贷（贷款服务）"
        or bao.product_name = "大道易贷（贷款服务）"
        or bao.product_name = "限时贷"
        or bao.product_name like "%拍卖保%"
          )
      then tbnl.max_biz_loan_amount

     else 0
end) fk_amount
-- ----------------------- 当日净收费（万元）-----------------------------------------------------------------
--  净收费 = charge_amount -  return_premium_amount
-- 
,tmp_csc1.max_trans_day charge_trans_date
,tmp_csc1.trans_money charge_amount
,tmp_csd1.max_trans_day return_premium_date 
,tmp_csd1.trans_money return_premium_amount

-- -------------------------------在途金额-------------------------------------------

,case when (bao.product_name like '%交易保%' 
      or bao.product_name like "%提放保%"
      or bao.product_name like "%拍卖保%"
      or bao.product_name like "%买付保%"
      ) and  bim.insurance_release_time is null then 1

     when bao.product_name like "%及时贷%"  
        and bao.product_name not like "%贷款服务%" 
        and tmp_cc01.max_trans_day is null then 1 
       else 0         
 end order_flag


,case when (bao.product_name like '%交易保%' 
      or bao.product_name like "%提放保%"
      or bao.product_name like "%拍卖保%"
      or bao.product_name like "%买付保%"
      ) 
     then bim.insurance_release_time 

     when bao.product_name like "%及时贷%"  and bao.product_name not like "%贷款服务%" 
     then tmp_cc01.max_trans_day
                
 end khhk_date --  客户回款

 ,qjjx.firstPartOfChargingTerm 
 ,qjjx.chargingTermGap
 ,qjjx.channelPricePerOrderFirstPart
 ,qjjx.channelPricePerOrderPerPart

from ods.ods_bpms_biz_apply_order_common bao
LEFT JOIN ods.ods_bpms_biz_isr_mixed bim on bao.apply_no = bim.apply_no
left join ods.ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
left join (select * from ods.ods_bpms_biz_ransom_floor_common where rn=1 ) brf on brf.apply_no=bao.apply_no
left join ods.ods_bpms_sys_org sai on bao.branch_id = sai.CODE_
left join ods.ods_bpms_sys_org so on bao.sales_branch_id = so.CODE_
left join ods.ods_bpms_sys_user su on bao.sales_user_id = su.id_
left join tmp_qjjx qjjx on bao.apply_no = qjjx.apply_no
left join tmp_cc01 on bao.apply_no = tmp_cc01.apply_no
left join tmp_order_matter_record tomr on bao.apply_no = tomr.apply_no
left join tmp_biz_new_loan tbnl on bao.house_no = tbnl.house_no
left join tmp_csc1 on bao.apply_no = tmp_csc1.apply_no
left join tmp_csd1 on bao.apply_no = tmp_csd1.apply_no
where bao.product_id in (
"AJFW_NSL_YJY",
"DDKD_NJY_OTH",
"DDKD_NJY_OTH_CPGD",
"DDKD_NJY_SER",
"DDKD_NSL_NJY_SER",
"IMFB_NSL_YJY_ISR",
"IMFB_YSL_YJY_ISR",
"JSD_NSL_NJY_CSH",
"JSD_NSL_NJY_SER",
"JSD_NSL_YJY_CSH",
"JYB_NSL_YJY_ISR",
"JYB_YSL_YJY_ISR",
"MFB_YSL_YJY_ISR",
"PMB_YJY_ISR",
"SLY_YSL_NJY_CSH",
"SLY_YSL_YJY_CSH",
"SSD_NJY_SER",
"TFB_NSL_NJY_ISR",
"TFB_YSL_NJY_ISR",
"XSD_YJY_OTH",
"DZYB_YSL_YJY_ISR",
"ZYB_YSL_NJY_ISR",
"ZYB_YSL_YJY_ISR"
)
and bao.product_name in(
'买付保（无赎楼）',
'买付保（有赎楼）',
'买付保（有赎楼）（大道保障模式）',
'交易保（无赎楼）',
'交易保（有赎楼）',
'及时贷（交易提放）',
'及时贷（交易赎楼）',
'及时贷（贷款服务）',
'及时贷（非交易提放）',
'及时贷（非交易赎楼）',
'大道快贷（贷款服务）',
'大道按揭',
'大道易贷（贷款服务）',
'抵押代办',
'拍卖保',
'提放保（无赎楼）',
'提放保（有赎楼）',
'限时贷',
'交易保(两笔)-保险',
'提放保(两笔)-保险',
'交易保(两笔)-担保'
);