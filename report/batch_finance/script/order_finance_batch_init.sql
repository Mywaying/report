set hive.execution.engine=spark;
use ods;
drop table if exists dwd.dwdtmp_order_finance;
CREATE TABLE dwd.dwdtmp_order_finance (
  apply_no STRING COMMENT '订单编号',
  product_name STRING COMMENT '产品名称',
  apply_status STRING COMMENT '业务状态',
  partner_insurance_name STRING COMMENT '合作机构/保险',
  partner_bank_name STRING COMMENT '合作银行',
  product_term bigint comment '产品期限',
  sales_user_name STRING COMMENT '渠道经理',
  biz_loan_amount DOUBLE COMMENT '新商贷金额',
  interview_date timestamp COMMENT '面签时间',
  shenqing_amount DOUBLE COMMENT '赎楼/借款金额',
  product_term_and_charge_way STRING COMMENT '收费方式',
  fixed_term bigint comment '预估用款期限',
  guanlian_type STRING COMMENT '关联类型',
  group_apply_no STRING COMMENT '主关联订单',
  risk_grade STRING COMMENT '风险等级',
  price_tag STRING COMMENT '价格标签',
  channel_tag STRING COMMENT '渠道标签',
  fund_use_date_num bigint comment '实借天数',
  channel_price DOUBLE COMMENT '渠道价费率',
  agent_commission DOUBLE COMMENT '代收返佣费率',
  overdue_rate DOUBLE COMMENT '超期费率',
  htflhj DOUBLE COMMENT '合同费率合计',
  borrowing_due_date_cust timestamp COMMENT '(客户)预计借款回款日',
  borrowing_due_date_platform timestamp COMMENT '(平台)借款到期日',
  payee_acct_jiaofei STRING COMMENT '服务费-缴费-收款户名',
  payee_bank_name_jiaofei STRING COMMENT '服务费-缴费-收款银行',
  payee_acct_no_jiaofei STRING COMMENT '服务费-缴费-收款账号',
  jiaofei_date string COMMENT '服务费-缴费-转入日期',
  jiaofei_amt DOUBLE COMMENT '服务费-缴费-转入金额',
  payee_acct_tuifei STRING COMMENT '服务费-退费-收款户名',
  payee_bank_name_tuifei STRING COMMENT '服务费-退费-收款银行',
  payee_acct_no_tuifei STRING COMMENT '服务费-退费-收款账号',
  trans_day_tuifei STRING COMMENT '服务费-退费-转出日期',
  trans_money_tuifei double COMMENT '服务费-退费-转出金额',
  actual_amount_total DOUBLE COMMENT '总收费-实收',
  zsf_ys DOUBLE COMMENT '总收费-应收',
  reduction_amount DOUBLE COMMENT '总收费-减免金额',
  differ_total DOUBLE COMMENT '总收费-差额',
  total_remark STRING COMMENT '总收费-备注',
  pyjt_amount DOUBLE COMMENT '大道服务费-其中:偏远交通费',
  gsfwf_ajfwf DOUBLE COMMENT '大道服务费-其中:按揭服务费',
  ddfwf_ys DOUBLE COMMENT '大道服务费-应收',
  company_service DOUBLE COMMENT '大道服务费-其中:服务费',
  ddfwf_fjfwf DOUBLE COMMENT '大道服务费-其中:附加服务费',
  company_rebate DOUBLE COMMENT '大道服务费-其中:汇思返佣+发票',
  company_collect_rabate DOUBLE COMMENT '大道服务费-代收代付',
  qzqdfy_amount DOUBLE COMMENT '大道服务费-应返返佣',
  company_collection DOUBLE COMMENT '大道服务费-实收',
  ddfwf_check DOUBLE COMMENT '大道服务费-Check',
  company_remark STRING COMMENT '大道服务费-备注',
  ddfwf_ce DOUBLE COMMENT '大道服务费-差额(实际服务费)',
  premium_amount DOUBLE COMMENT '保费-应收',
  insurance_is_collect STRING COMMENT '保费-是否代收',
  insurance_remark STRING COMMENT '保费-备注',
  customer_fee_total DOUBLE COMMENT '客户收费总额',
  actual_amount_rebate DOUBLE COMMENT '保险返点-实收',
  insurance_rebate_remark STRING COMMENT '保险返点-备注',
  should_plat_cost DOUBLE COMMENT '平台应收-服务费',
  ought_amount_plat_interest DOUBLE COMMENT '平台应收-利息',
  assess_receivable DOUBLE COMMENT '代收代付-评估费-应收',
  assess_collect DOUBLE COMMENT '代收代付-评估费-实收',
  assess_pay DOUBLE COMMENT '代收代付-评估费-支付金额',
  assess_pay_time timestamp COMMENT '代收代付-评估费-支付时间',
  assess_remark STRING COMMENT '代收代付-评估费-备注',
  print_receivable DOUBLE COMMENT '代收代付-印花税-应收',
  print_collect DOUBLE COMMENT '代收代付-印花税-实收',
  print_pay DOUBLE COMMENT '代收代付-印花税-支付金额',
  print_pay_time timestamp COMMENT '代收代付-印花税-支付时间',
  print_pay_remark STRING COMMENT '代收代付-印花税-备注',
  fair_receivable DOUBLE COMMENT '代收代付-公证费-应收',
  fair_collect DOUBLE COMMENT '代收代付-公证费-实收',
  fair_pay DOUBLE COMMENT '代收代付-公证费-支付金额',
  fair_pay_time timestamp COMMENT '代收代付-公证费-支付时间',
  fair_pay_remark STRING COMMENT '代收代付-公证费-备注',
  receive_name_plat_fk STRING COMMENT '平台/银行放款-收款户名',
  receive_bank_plat_fk STRING COMMENT '平台/银行放款-收款银行',
  receive_acct_no_plat_fk STRING COMMENT '平台/银行放款-收款账号',
  realease_amount DOUBLE COMMENT '平台/银行放款-到账金额',
  out_name_hb STRING COMMENT '资金划拨-出款户名',
  out_bank_hb STRING COMMENT '资金划拨-出款银行',
  out_acct_no_hb STRING COMMENT '资金划拨-出款账号',
  receive_name_hb STRING COMMENT '资金划拨-收款户名',
  receive_bank_hb STRING COMMENT '资金划拨-收款银行',
  receive_acct_no_hb STRING COMMENT '资金划拨-收款账号',
  huabo_date timestamp COMMENT '资金划拨-划出日期',
  huabo_amount double COMMENT '资金划拨-划出金额',
  sfwk_date timestamp COMMENT '资金划拨-释放尾款时间',
  advance_before_shulou STRING COMMENT '是否赎楼前垫资',
  flooradvance_adv_amt DOUBLE COMMENT '赎楼前垫资-垫资金额',
  flooradvance_fin_date timestamp COMMENT '赎楼前垫资-确认时间',
  flooradvance_ret_date timestamp COMMENT '赎楼前垫资-回款时间',
  advance_return STRING COMMENT '是否到期归还垫资',
  expireadvance_adv_amt DOUBLE COMMENT '到期归还垫资-垫资金额',
  expireadvance_fin_date timestamp COMMENT '到期归还垫资-出款时间',
  expireadvance_ret_date timestamp COMMENT '到期归还垫资-回款时间',
  advance_day STRING COMMENT '到期垫资罚息-垫资天数',
  advance_penalty DOUBLE COMMENT '到期垫资罚息-罚息率/天',
  yingshoufaxijine DOUBLE COMMENT '到期垫资罚息-应收罚息',
  advance_collection DOUBLE COMMENT '到期垫资罚息-实收罚息',
  advance_colle_time timestamp COMMENT '到期垫资罚息-实收时间',
  receive_name_hk STRING COMMENT '(客户)借款回款-收款户名',
  receive_bank_hk STRING COMMENT '(客户)借款回款-收款银行',
  receive_acct_no_hk STRING COMMENT '(客户)借款回款-收款账号',
  receive_amount_hk DOUBLE COMMENT '(客户)借款回款-收款金额',
  rebate_date_hk_all STRING COMMENT '(客户)借款回款-回款日期',
  out_name STRING COMMENT '借款归还-出款户名',
  cc02_out_bank_hb STRING COMMENT '借款归还-出款银行',
  cc02_out_acct_no_hb STRING COMMENT '借款归还-出款账号',
  cc02_receive_name_guihuan STRING COMMENT '借款归还-收款户名',
  cc02_receive_bank_guihuan STRING COMMENT '借款归还-收款银行',
  cc02_receive_acct_no_guihuan STRING COMMENT '借款归还-收款账号',
  fact_plat_cost DOUBLE COMMENT '借款归还-平台费',
  check_02 DOUBLE COMMENT '借款归还-check',
  loan_interest DOUBLE COMMENT '借款归还-借款利息',
  early_repay_faxi DOUBLE COMMENT '借款归还-提前还款罚息',
  check_01 DOUBLE COMMENT '借款归还-check(平台费)',
  return_amount DOUBLE COMMENT '借款归还-归还金额',
  return_date timestamp COMMENT '借款归还-归还日期',
  channel_name STRING COMMENT '审批及报销-渠道公司',
  approval_compen_way STRING COMMENT '审批及报销-报销方式',
  approval_check DOUBLE COMMENT '审批及报销-check',
  humanpool_rebate DOUBLE COMMENT '汇思支付-返佣金额',
  humanpool_collect_day STRING COMMENT '汇思支付-实付日期',
  invoice_rebate DOUBLE COMMENT '发票报销-返佣金额',
  invoice_submit_day STRING COMMENT '发票报销-提交总部日期',
  invoice_collect_day STRING COMMENT '发票报销-实付日期',
  replace_turn_out DOUBLE COMMENT '代收代付返佣-转出金额',
  replace_turn_day STRING COMMENT '代收代付返佣-转出日期',
  plat_money DOUBLE COMMENT '平台费对账-平台金额',
  compare_amount DOUBLE COMMENT '平台费对账-差额',
  plat_check_day STRING COMMENT '平台费对账-对账日期',
  plat_collection DOUBLE COMMENT '平台费对账-实付金额',
  plat_collect_day STRING COMMENT '平台费对账-实付日期',
  guidang_date timestamp COMMENT '财务归档时间',
  guidang_user STRING COMMENT '财务归档经办人',
  borrowing_value_date timestamp COMMENT '(客户)借款起息日',
  is_jms STRING COMMENT '是否加盟',
  platform_value_date timestamp COMMENT '(平台)借款起息日',
  profits_money DOUBLE COMMENT '合计-分润金额',
  city_name STRING COMMENT '分公司',
  branch_name STRING COMMENT '子公司',
  product_type string comment "产品类型",
  insurance_release_time timestamp comment '解保时间',
  is_fund_package string comment "是否资金包",
  other_amount double comment "公司服务费-其他金额" ,
  price_discount string comment "定价折扣" ,
  seller_name_all string comment "卖方姓名(全)",
  buyer_name_all string comment "买方姓名(全)",
  tail_release_node_name string comment '放款节点',
  policy_no string comment '保单号码',
  dd_2_insur_premium_amount double comment '保费-实缴',
  ss_premium_amount double comment '保费-实收',
  is_create_policy_no string comment '是否出保单',
  mortgage_collect double comment '代收代付-抵押登记费-实收',
  mortgage_receivable double comment '代收代付-抵押登记费-应收',
  mortgage_pay double comment '代收代付-抵押登记费-支付金额',
  mortgage_pay_time timestamp comment '代收代付-抵押登记费-支付时间',
  mortgage_pay_remark string comment '代收代付-抵押登记费-备注',
  loan_time_jc timestamp comment '平台/银行放款-到账时间_计财',
  loan_time_xg timestamp comment '放款时间_销管',
  payer_acct_jiaofei string comment '服务费-缴费-出款户名',
  payer_acct_no_jiaofei string comment '服务费-缴费-出款账号',
  payer_bank_name_jiaofei string comment '服务费-缴费-出款银行',
  payer_acct_no_tuifei string comment '服务费-退费-出款账号',
  humanpool_payment_day timestamp  comment '汇思支付-提交总部日期',
  actualInsurancePremium double comment '保费-应收(实际)',
  profit_exceed double comment '校验差额',
  return_status string comment '回款状态',
  financial_time timestamp comment '校验差额时间',
  guarantee_fee DOUBLE COMMENT '担保费-应收',
  guarantee_fee_return DOUBLE COMMENT '担保费-预估返还',
  max_payment_time TIMESTAMP COMMENT '(客户)最终回款日',
  qdj_insur_b double comment '渠道价(保险)/笔(%)',
  bffc_bl double comment '保费分成比例(%)',
  ad_pay_time	TIMESTAMP	COMMENT	'赎楼前垫资回款时间',
  funded_repayment_amount	DOUBLE	COMMENT	'到期归还垫资-回款金额',
  agency_difference_cw DOUBLE COMMENT '合计-实收渠道费',
  agency_channel_cw DOUBLE COMMENT '合计-代收渠道费',
  should_plat_cost_cw DOUBLE COMMENT '合计-平台服务费',
  ought_amount_plat_interest_cw DOUBLE COMMENT '合计-平台利息',
  cost_capital DOUBLE COMMENT '合计-资金成本',
  ftp_pre_rate double comment '前置费率',
  ftp_pre_amount double comment '前置金额',
  company_service_fee DOUBLE COMMENT '合计-公司服务费',
  headquarters_operating_income DOUBLE COMMENT '合计-总部营收',
  cost DOUBLE COMMENT '合计-成本',
  income DOUBLE COMMENT '合计-收入',
  branch_operating_income DOUBLE COMMENT '合计-分公司营收',
  financingcostrateperday_fee_value double comment '担保费/天（%）',
  guaranteefeerateperday_fee_value double comment '融资成本率/天（%）',
  new_loan_rate double comment '贷款利率',
  rank bigint comment '辅助列'
);
insert overwrite table dwd.`dwdtmp_order_finance`
select abc.apply_no,
abc.product_name,
abc.apply_status,
abc.partner_insurance_name,
abc.partner_bank_name,
abc.product_term,
abc.sales_user_name,
abc.biz_loan_amount,
abc.interview_date,
abc.shenqing_amount,
abc.product_term_and_charge_way,
abc.fixed_term,
abc.guanlian_type,
abc.group_apply_no,
abc.risk_grade,
abc.price_tag,
abc.channel_tag,
abc.fund_use_date_num,
abc.channel_price,
abc.agent_commission,
abc.overdue_rate,
abc.htflhj,
abc.borrowing_due_date_cust,
abc.borrowing_due_date_platform,
abc.payee_acct_jiaofei,
abc.payee_bank_name_jiaofei,
abc.payee_acct_no_jiaofei,
abc.jiaofei_date,
abc.jiaofei_amt,
abc.payee_acct_tuifei,
abc.payee_bank_name_tuifei,
abc.payee_acct_no_tuifei,
abc.trans_day_tuifei,
abc.trans_money_tuifei,
abc.actual_amount_total,
abc.zsf_ys,
abc.reduction_amount,
abc.differ_total,
abc.total_remark,
abc.pyjt_amount,
abc.gsfwf_ajfwf,
abc.ddfwf_ys,
abc.company_service,
abc.ddfwf_fjfwf,
abc.company_rebate,
abc.company_collect_rabate,
abc.qzqdfy_amount,
abc.company_collection,
abc.ddfwf_check,
abc.company_remark,
abc.ddfwf_ce,
abc.premium_amount,
abc.insurance_is_collect,
abc.insurance_remark,
abc.customer_fee_total,
abc.actual_amount_rebate,
abc.insurance_rebate_remark,
abc.should_plat_cost,
abc.ought_amount_plat_interest,
abc.assess_receivable,
abc.assess_collect,
abc.assess_pay,
abc.assess_pay_time,
abc.assess_remark,
abc.print_receivable,
abc.print_collect,
abc.print_pay,
abc.print_pay_time,
abc.print_pay_remark,
abc.fair_receivable,
abc.fair_collect,
abc.fair_pay,
abc.fair_pay_time,
abc.fair_pay_remark,
abc.receive_name_plat_fk,
abc.receive_bank_plat_fk,
abc.receive_acct_no_plat_fk,
abc.realease_amount,
abc.out_name_hb,
abc.out_bank_hb,
abc.out_acct_no_hb,
abc.receive_name_hb,
abc.receive_bank_hb,
abc.receive_acct_no_hb,
abc.huabo_date,
abc.huabo_amount,
abc.sfwk_date,
abc.advance_before_shulou,
abc.flooradvance_adv_amt,
abc.flooradvance_fin_date,
abc.flooradvance_ret_date,
abc.advance_return,
abc.expireadvance_adv_amt,
abc.expireadvance_fin_date,
abc.expireadvance_ret_date,
abc.advance_day,
abc.advance_penalty,
abc.yingshoufaxijine,
abc.advance_collection,
abc.advance_colle_time,
abc.receive_name_hk,
abc.receive_bank_hk,
abc.receive_acct_no_hk,
abc.receive_amount_hk,
abc.rebate_date_hk_all,
abc.out_name,
abc.cc02_out_bank_hb,
abc.cc02_out_acct_no_hb,
abc.cc02_receive_name_guihuan,
abc.cc02_receive_bank_guihuan,
abc.cc02_receive_acct_no_guihuan,
abc.fact_plat_cost,
abc.check_02,
abc.loan_interest,
abc.early_repay_faxi,
abc.check_01,
abc.return_amount,
abc.return_date,
abc.channel_name,
abc.approval_compen_way,
abc.approval_check,
abc.humanpool_rebate,
abc.humanpool_collect_day,
abc.invoice_rebate,
abc.invoice_submit_day,
abc.invoice_collect_day,
abc.replace_turn_out,
abc.replace_turn_day,
abc.plat_money,
abc.compare_amount,
abc.plat_check_day,
abc.plat_collection,
abc.plat_collect_day,
abc.guidang_date,
abc.guidang_user,
abc.borrowing_value_date,
abc.is_jms,
abc.platform_value_date,
abc.profits_money,
abc.city_name,
abc.branch_name,
abc.product_type,
abc.insurance_release_time,
abc.is_fund_package,
abc.other_amount,
abc.price_discount,
abc.seller_name_all,
abc.buyer_name_all,
abc.tail_release_node_name,
abc.policy_no,
abc.dd_2_insur_premium_amount,
abc.ss_premium_amount,
abc.is_create_policy_no,
abc.mortgage_collect,
abc.mortgage_receivable,
abc.mortgage_pay,
abc.mortgage_pay_time,
abc.mortgage_pay_remark,
abc.loan_time_jc,
abc.loan_time_xg,
abc.payer_acct_jiaofei,
abc.payer_acct_no_jiaofei,
abc.payer_bank_name_jiaofei,
abc.payer_acct_no_tuifei,
abc.humanpool_payment_day,
abc.actualInsurancePremium,
abc.profit_exceed,
abc.return_status,
abc.financial_time,
abc.guarantee_fee,
abc.guarantee_fee_return,
abc.max_payment_time,
abc.qdj_insur_b,
abc.bffc_bl,
abc.ad_pay_time,
abc.funded_repayment_amount,
abc.agency_difference_cw,
abc.agency_channel_cw,
abc.should_plat_cost_cw,
abc.ought_amount_plat_interest_cw,
abc.cost_capital,
abc.ftp_pre_rate,
abc.ftp_pre_amount,
abc.company_service_fee,
abc.headquarters_operating_income,
abc.cost,
abc.income,
abc.branch_operating_income,
abc.financingcostrateperday_fee_value,
abc.guaranteefeerateperday_fee_value,
abc.new_loan_rate,
abc.rank
from(

  select 
  a.*
  ,(nvl(a.`company_service_fee`,0)+nvl(a.`agency_difference_cw`,0)+nvl(a.`agency_channel_cw`,0)+nvl(a.`should_plat_cost_cw`,0)
      -nvl(a.profits_money, 0))/1.06  AS income

  ,(nvl(a.`company_service_fee`,0)+nvl(a.`agency_difference_cw`,0)+nvl(a.`agency_channel_cw`,0)+nvl(a.`should_plat_cost_cw`,0)
      -nvl(a.profits_money, 0))/1.06 - nvl(a.cost_capital, 0) + nvl(a.plat_interest, 0) as  branch_operating_income --分公司营业收入
  ,row_number() over(partition by apply_no order by apply_time desc) rank
  from (
      select 
      a.*
      ,b.agency_difference agency_difference_cw  --  实收渠道费
      ,b.agency_channel agency_channel_cw --  代收渠道费
      ,b.should_plat_cost should_plat_cost_cw --  平台服务费
      ,b.ought_amount_plat_interest ought_amount_plat_interest_cw --  平台利息
      ,b.cost_capital --  资金成本
      ,b.ftp_pre_rate -- 前置利率
      ,b.ftp_pre_amount -- 前置金额
      ,case when a.product_term_and_charge_way = "固定期限" or a.product_term_and_charge_way = '固定费率' 
          then 
              nvl(a.customer_fee_total, 0)
              - nvl(a.reduction_amount, 0) -- 减免金额
              - nvl(b.agency_difference, 0) -- 分摊实收渠道费
              - nvl(b.agency_channel, 0) -- 分摊代收渠道费
              - nvl(b.should_plat_cost, 0) -- 分摊平台服务费
              - nvl(b.ought_amount_plat_interest, 0) -- 分摊平台利息
              - nvl(a.print_receivable, 0) -- 代收代付：印花税 - 应收（元）
              - nvl(a.assess_receivable, 0) -- 代收代付：评估费 - 应收（元）
              - nvl(a.fair_receivable, 0) -- 代收代付：公证费 - 应收（元）

          when a.product_term_and_charge_way = '固定期限' and a.apply_status = '完结' then a.ddfwf_ys 

          
          when a.product_term_and_charge_way = '按天计息' and a.max_payment_time is null then 
              (nvl(a.customer_fee_total, 0)
              - nvl(a.reduction_amount, 0) -- 减免金额 
              - nvl(b.agency_difference, 0) -- 分摊实收渠道费
              - nvl(b.agency_channel, 0) -- 分摊代收渠道费
              - nvl(b.should_plat_cost, 0) -- 分摊平台服务费
              - nvl(b.ought_amount_plat_interest, 0) -- 分摊平台利息
              - nvl(a.print_receivable, 0) --应收（元）
              - nvl(a.assess_receivable, 0) -- 代收代付：评估费 - 应收（元）
              - nvl(a.fair_receivable, 0) -- 代收代付：公证费 - 应收（元）
              )/nvl(a.fixed_term, 0)*nvl(a.fund_use_date_num,0)

          when a.product_term_and_charge_way = '按天计息' and a.apply_status = '完结' then a.ddfwf_ys 

          when a.product_term_and_charge_way = '区间计息' and a.max_payment_time is null 
          then 
              nvl(a.customer_fee_total, 0)
              - nvl(a.reduction_amount, 0) -- 减免金额
              - nvl(b.agency_difference, 0) -- 分摊实收渠道费
              - nvl(b.agency_channel, 0) -- 分摊代收渠道费
              - nvl(b.should_plat_cost, 0) -- 分摊平台服务费
              - nvl(b.ought_amount_plat_interest, 0) -- 分摊平台利息
              - nvl(a.print_receivable, 0) -- 代收代付：印花税 - 应收（元）
              - nvl(a.assess_receivable, 0) -- 代收代付：评估费 - 应收（元）
              - nvl(a.fair_receivable, 0) -- 代收代付：公证费 - 应收（元）

          when a.product_term_and_charge_way = '区间计息' and a.apply_status = '完结' then a.ddfwf_ys 
   
          end as company_service_fee -- 公司服务费-实收

        ,(case 
          when a.max_payment_time  is not null then b.ought_amount_plat_interest
          when a.max_payment_time  is null and a.advance_before_shulou='是' and a.advance_return ='否' and a.product_term_and_charge_way in ('固定期限', '固定费率', '区间计息' )
            then b.ought_amount_plat_interest - a.fixed_term/a.`fund_use_date_num` * a.floorAdvance_adv_amt/a.realease_amount*b.ought_amount_plat_interest
          when a.max_payment_time is null and a.advance_before_shulou ='否' and a.advance_return ='是'  then b.ought_amount_plat_interest
          when a.max_payment_time is null and a.advance_before_shulou ='否' and a.advance_return ='否'  then b.ought_amount_plat_interest
          when a.max_payment_time is null and a.advance_before_shulou ='是' and a.platform_value_date is null and a.realease_amount = a.floorAdvance_adv_amt then 0
          when a.max_payment_time is null and a.advance_before_shulou ='是' and a.platform_value_date is null and a.realease_amount > a.floorAdvance_adv_amt and a.product_term_and_charge_way in ('固定期限', '固定费率', '区间计息' )
          then b.ought_amount_plat_interest - a.fixed_term/a.fund_use_date_num * a.floorAdvance_adv_amt/a.realease_amount*b.ought_amount_plat_interest
        when a.max_payment_time is null and a.advance_before_shulou ='是' and a.advance_return = '是' then b.ought_amount_plat_interest
        when a.max_payment_time is null and a.advance_before_shulou='是' and a.advance_return = '否' and a.product_term_and_charge_way = '按天计息' 
          then b.ought_amount_plat_interest - datediff(a.floorAdvance_ret_date, a.floorAdvance_fin_date)/a.`fund_use_date_num` * a.floorAdvance_adv_amt/a.realease_amount * b.ought_amount_plat_interest
          when a.max_payment_time is null and a.advance_before_shulou='是' and a.platform_value_date is null and  a.realease_amount>a.floorAdvance_adv_amt and a.product_term_and_charge_way = '按天计息'
          then b.ought_amount_plat_interest - datediff(a.floorAdvance_ret_date, a.floorAdvance_fin_date)/a.fund_use_date_num * a.floorAdvance_adv_amt/a.realease_amount * b.ought_amount_plat_interest
        else null 
      end ) as plat_interest -- 平台利息


      ,(b.cost_capital
       -nvl((case when a.max_payment_time  is not null then b.ought_amount_plat_interest
                when a.max_payment_time is null and a.advance_before_shulou='是' and a.advance_return ='否'
                  then b.ought_amount_plat_interest- datediff(a.floorAdvance_ret_date, a.floorAdvance_fin_date)/a.`fixed_term`*a.floorAdvance_adv_amt/a.realease_amount*b.ought_amount_plat_interest
                when a.max_payment_time is null and a.advance_before_shulou ='否' and a.advance_return ='是' then b.ought_amount_plat_interest
                when a.max_payment_time is null and a.advance_before_shulou ='否' and a.advance_return ='否' then b.ought_amount_plat_interest
                when a.max_payment_time is null and a.advance_before_shulou ='是' and a.platform_value_date is null and a.realease_amount=a.floorAdvance_adv_amt then 0
                when a.max_payment_time is null and a.advance_before_shulou ='是' and a.platform_value_date is null and a.realease_amount>a.floorAdvance_adv_amt 
                    then b.ought_amount_plat_interest - datediff(a.floorAdvance_ret_date, a.floorAdvance_fin_date)/a.`fixed_term`*a.floorAdvance_adv_amt/a.realease_amount*b.ought_amount_plat_interest
                when a.max_payment_time is null and a.advance_before_shulou ='是' and a.advance_return ='是'  then b.ought_amount_plat_interest
                else null 
            end ),0)) as headquarters_operating_income --总部营业收入
      -- 分摊实收渠道费 + 分摊代收渠道费 + 分摊平台服务费
      ,case when a.product_type = "保险类产品"  
              then (CASE WHEN a.`bank_loan_time` is not null or a.`insurance_release_time` is not null 
                   THEN nvl(a.`company_rebate`,0)+nvl(a.`company_collect_rabate`,0)
                   ELSE null END )
            else nvl(b.agency_difference, 0) + nvl(b.agency_channel, 0) + nvl(b.should_plat_cost, 0) 
       end cost -- 成本
    from dwd.tmp_dwd_order_finance a
    left join dwd.tmp_dwd_order_finance_2 b on a.apply_no = b.apply_no
  ) as a
) as abc
where abc.rank = 1;
drop table if exists dwd.dwd_order_finance;
ALTER TABLE dwd.dwdtmp_order_finance RENAME TO dwd.dwd_order_finance;