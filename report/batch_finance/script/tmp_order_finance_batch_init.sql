set hive.execution.engine=spark;
drop table if exists dwd.tmp_dwd_order_finance;
CREATE TABLE dwd.tmp_dwd_order_finance (   
  apply_no STRING COMMENT '订单编号',
  product_name STRING COMMENT '产品名称',
  apply_status STRING COMMENT '订单状态',
  partner_insurance_name STRING COMMENT '合作机构_合作保险',
  partner_bank_name STRING COMMENT '合作银行',
  product_term bigint comment '借款期限',
  sales_user_name STRING COMMENT '渠道经理',
  biz_loan_amount DOUBLE COMMENT '买家按揭',
  interview_date timestamp COMMENT '面签时间',
  shenqing_amount DOUBLE COMMENT '申请金额',
  product_term_and_charge_way STRING COMMENT '收费方式',
  fixed_term bigint comment '客户收费期限',
  guanlian_type STRING COMMENT '关联类型',
  group_apply_no STRING COMMENT '关联id',
  risk_grade STRING COMMENT '风险标签',
  price_tag STRING COMMENT '价格标签',
  channel_tag STRING COMMENT '渠道标签',
  fund_use_date_num bigint comment '实际借款期限',
  channel_price DOUBLE COMMENT '渠道价费率',
  agent_commission DOUBLE COMMENT '代收返佣费率',
  overdue_rate DOUBLE COMMENT '超期费率',
  htflhj DOUBLE COMMENT '合同费率合计',
  borrowing_due_date_cust timestamp COMMENT '借款到期日-客户收费',
  borrowing_due_date_platform timestamp COMMENT '借款到期日-平台发标',
  payee_acct_jiaofei STRING COMMENT '服务费-缴费-收款户名',
  payee_bank_name_jiaofei STRING COMMENT '服务费-缴费-收款银行',
  payee_acct_no_jiaofei STRING COMMENT '服务费-缴费-收款帐号',
  jiaofei_date string COMMENT '服务费-缴费-转入日期',
  jiaofei_amt DOUBLE COMMENT '服务费-缴费-转入金额',
  payee_acct_tuifei STRING COMMENT '服务费-退费-收款户名',
  payee_bank_name_tuifei STRING COMMENT '服务费-退费-收款银行',
  payee_acct_no_tuifei STRING COMMENT '服务费-退费-收款帐号',
  trans_day_tuifei STRING COMMENT '服务费-退费-转出日期',
  trans_money_tuifei double COMMENT '服务费-退费-转出金额',
  actual_amount_total DOUBLE COMMENT '总收费-实收',
  zsf_ys DOUBLE COMMENT '总收费-应收',
  reduction_amount DOUBLE COMMENT '总收费-减免金额',
  total_remark STRING COMMENT '总收费-备注',
  pyjt_amount DOUBLE COMMENT '公司服务费-其中：偏远交通费',
  gsfwf_ajfwf DOUBLE COMMENT '公司服务费-其中：按揭服务费',
  company_service DOUBLE COMMENT '大道服务费-其中：服务费',
  ddfwf_fjfwf DOUBLE COMMENT '大道服务费-其中：附加服务费',
  company_rebate DOUBLE COMMENT '大道服务费-其中：汇思返佣+发票',
  qzqdfy_amount DOUBLE COMMENT '大道服务费-应返返佣金额',
  ddfwf_check DOUBLE COMMENT '大道服务费-Check',
  company_remark STRING COMMENT '大道服务费-备注',
  ddfwf_ce DOUBLE COMMENT '大道服务费-差额(实际服务费）',
  premium_amount DOUBLE COMMENT '保费-应收',
  insurance_is_collect STRING COMMENT '保费-是否代收',
  insurance_remark STRING COMMENT '保费-备注',
  actual_amount_rebate DOUBLE COMMENT '保险公司返点-应收金额',
  insurance_rebate_remark STRING COMMENT '保险公司返点-备注',
  should_plat_cost DOUBLE COMMENT '平台服务费-应收',
  ought_amount_plat_interest DOUBLE COMMENT '平台利息-应收',
  assess_receivable DOUBLE COMMENT '代收代付：评估费-应收',
  assess_collect DOUBLE COMMENT '代收代付：评估费-实收',
  assess_pay DOUBLE COMMENT '代收代付：评估费-支付金额',
  assess_pay_time timestamp COMMENT '代收代付：评估费-支付时间',
  assess_remark STRING COMMENT '代收代付：评估费 - 备注',
  print_receivable DOUBLE COMMENT '代收代付：印花税 - 应收',
  print_collect DOUBLE COMMENT '代收代付：印花税 - 实收',
  print_pay DOUBLE COMMENT '代收代付：印花税 - 支付金额',
  print_pay_time timestamp COMMENT '代收代付：印花税 -支付时间',
  print_pay_remark STRING COMMENT '代收代付：印花税 - 备注',
  fair_receivable DOUBLE COMMENT '代收代付：公证费 - 应收',
  fair_collect DOUBLE COMMENT '代收代付：公证费 - 实收',
  fair_pay DOUBLE COMMENT '代收代付：公证费 - 支付金额',
  fair_pay_time timestamp COMMENT '代收代付：公证费 -支付时间',
  fair_pay_remark STRING COMMENT '代收代付：公证费 - 备注',
  receive_name_plat_fk STRING COMMENT '放款（到赎楼共管户）-收款户名',
  receive_bank_plat_fk STRING COMMENT '放款（到赎楼共管户）-收款银行',
  receive_acct_no_plat_fk STRING COMMENT '放款（到赎楼共管户）-收款帐号',
  realease_amount DOUBLE COMMENT '放款（到赎楼共管户）-到帐金额',
  out_name_hb STRING COMMENT '资金划拨-出款户名',
  out_bank_hb STRING COMMENT '资金划拨-出款银行',
  out_acct_no_hb STRING COMMENT '资金划拨-出款帐号 ',
  receive_name_hb STRING COMMENT '资金划拨-收款户名',
  receive_bank_hb STRING COMMENT '资金划拨-收款银行',
  receive_acct_no_hb STRING COMMENT '资金划拨-收款账号',
  huabo_date timestamp COMMENT '资金划拨-划出日期',
  huabo_amount double COMMENT '资金划拨-划出金额',
  sfwk_date timestamp COMMENT '赎楼信息-释放尾款时间',
  advance_before_shulou STRING COMMENT '垫资信息-是否赎楼前垫资',
  flooradvance_adv_amt DOUBLE COMMENT '垫资信息-赎楼前垫资金额',
  flooradvance_fin_date timestamp COMMENT '垫资信息-赎楼前垫资确认时间',
  flooradvance_ret_date timestamp COMMENT '垫资信息-赎楼前垫资回款时间',
  advance_return STRING COMMENT '垫资信息-是否到期归还垫资',
  expireadvance_adv_amt DOUBLE COMMENT '垫资信息-回款垫资金额',
  expireadvance_fin_date timestamp COMMENT '垫资信息-回款垫资出款时间',
  expireadvance_ret_date timestamp COMMENT '垫资信息-回款垫资回款时间',
  advance_day STRING COMMENT '到期垫资罚息收取信息-垫资天数',
  advance_penalty DOUBLE COMMENT '到期垫资罚息收取信息-罚息率/天',
  yingshoufaxijine DOUBLE COMMENT '到期垫资罚息收取信息-应收罚息金额',
  advance_collection DOUBLE COMMENT '到期垫资罚息收取信息-实收罚息金额',
  advance_colle_time timestamp COMMENT '到期垫资罚息收取信息-实收罚息时间',
  receive_name_hk STRING COMMENT '（客户）借款回款-收款户名',
  receive_bank_hk STRING COMMENT '（客户）借款回款-收款银行',
  receive_acct_no_hk STRING COMMENT '（客户）借款回款-收款帐号',
  receive_amount_hk DOUBLE COMMENT '（客户）借款回款-收款金额',
  rebate_date_hk_all STRING COMMENT '（客户）借款回款-回款日期',
  out_name STRING COMMENT '借款归还-出款户名',
  cc02_out_bank_hb STRING COMMENT '借款归还-出款银行',
  cc02_out_acct_no_hb STRING COMMENT '借款归还-出款账号',
  cc02_receive_name_guihuan STRING COMMENT '借款归还-收款户名',
  cc02_receive_bank_guihuan STRING COMMENT '借款归还-收款银行',
  cc02_receive_acct_no_guihuan STRING COMMENT '借款归还-收款帐号',
  fact_plat_cost DOUBLE COMMENT '借款归还-平台费',
  loan_interest DOUBLE COMMENT '借款归还-借款利息',
  early_repay_faxi DOUBLE COMMENT '借款归还-提前还款罚息',
  check_01 DOUBLE COMMENT '借款归还-check',
  return_amount DOUBLE COMMENT '借款归还-归还金额',
  return_date timestamp COMMENT '借款归还-归还日期',
  channel_name STRING COMMENT '审批及报销方式-渠道公司',
  approval_compen_way STRING COMMENT '审批及报销方式-报销方式',
  approval_check DOUBLE COMMENT '审批及报销方式-CHECK',
  humanpool_rebate DOUBLE COMMENT '汇思支付信息-返佣金额',
  humanpool_collect_day STRING COMMENT ' 汇思支付信息-实际付款日期',
  invoice_rebate DOUBLE COMMENT '发票报销信息-返佣金额',
  invoice_submit_day STRING COMMENT '发票报销信息-报销单提交总部日期',
  invoice_collect_day STRING COMMENT '发票报销信息-实际付款日期',
  replace_turn_out DOUBLE COMMENT '代收代付返佣-转出金额',
  replace_turn_day STRING COMMENT '代收代付返佣-转出日期',
  plat_money DOUBLE COMMENT '平台费对账-平台金额',
  plat_check_day STRING COMMENT '平台费对账-对账日期',
  plat_collection DOUBLE COMMENT '平台费对账-实际付款金额',
  plat_collect_day STRING COMMENT '平台费对账-实际付款日期',
  guidang_date timestamp COMMENT '财务归档时间',
  agency_channel DOUBLE COMMENT '中介代收渠道费',
  guidang_user STRING COMMENT '财务归档处理人',
  borrowing_value_date timestamp COMMENT '借款起息日',
  is_jms STRING COMMENT '是否加盟商业务',
  platform_value_date timestamp COMMENT '平台起息日',
  profits_money DOUBLE COMMENT '分润金额',
  city_name STRING COMMENT '分公司',
  branch_name STRING COMMENT '附属公司',
  apply_time timestamp COMMENT '订单申请时间', 
  product_type string comment "产品类型",
  bank_loan_time timestamp comment '银行放款时间',
  insurance_release_time timestamp comment '保险责任解除时间',
  is_fund_package string comment "是否资金包",
  other_amount double comment "公司服务费-其他金额" ,
  price_discount string comment "定价折扣" ,
  seller_name_all string comment "卖方所有人员姓名",
  buyer_name_all string comment "买方所有人员姓名",
  tail_release_node_name string comment '放款节点',
  policy_no string comment '保单号码',
  dd_2_insur_premium_amount double comment '实缴保费金额',
  ss_premium_amount double comment '实收保费',
  is_create_policy_no string comment '是否出保单',
  mortgage_collect double comment '代收代付：抵押登记费实收',
  mortgage_receivable double comment '代收代付：抵押登记费应收',
  mortgage_pay double comment '代收代付：抵押登记费支付金额',
  mortgage_pay_time timestamp comment '代收代付：抵押登记费支付时间',
  mortgage_pay_remark string comment '代收代付：抵押登记费备注',
  loan_time_jc timestamp comment '到账时间_计财',
  loan_time_xg timestamp comment '放款时间_销管',
  payer_acct_jiaofei string comment '服务费-缴费-出款户名',
  payer_acct_no_jiaofei string comment '服务费-缴费-出款账户',
  payer_bank_name_jiaofei string comment '服务费-缴费-出款银行',
  payer_acct_no_tuifei string comment '服务费-退费-出款账户',
  humanpool_payment_day timestamp  comment '汇思支付-提交总部日期',
  actualInsurancePremium double comment '保费-应收(实际)',
  profit_exceed double comment '校验差额',
  return_status string comment '回款状态',
  financial_time timestamp comment '校验差额时间',
  guarantee_fee DOUBLE COMMENT '担保费-应收',
  guarantee_fee_return DOUBLE COMMENT '担保费-预估返还',
  max_payment_time TIMESTAMP COMMENT '（客户）最新回款日',
  qdj_insur_b double comment '渠道价（保险）/笔（%）',
  bffc_bl double comment '保费分成比例（%）',
  ad_pay_time	TIMESTAMP	COMMENT	'赎楼前垫资回款时间',
  funded_repayment_amount	DOUBLE	COMMENT	'到期归还垫资-回款金额',
  financingcostrateperday_fee_value double comment '担保费/天（%）',
  guaranteefeerateperday_fee_value double comment '融资成本率/天（%）',
  new_loan_rate double comment '贷款利率',
  company_collect_rabate DOUBLE COMMENT '大道服务费-其中：代收代付返佣',
  customer_fee_total DOUBLE COMMENT '客户费用合计',
  differ_total DOUBLE COMMENT '总收费-差额',
  ddfwf_ys DOUBLE COMMENT '大道服务费-应收',
  company_collection DOUBLE COMMENT '大道服务费-实收',
  compare_amount DOUBLE COMMENT '平台费对账-差额',
  check_02 DOUBLE COMMENT '借款归还-check'
);
with t1 as (
  SELECT 
  t.`house_no`,
  concat_ws('/',collect_list(CASE WHEN t.type='GLK' THEN t.`name` ELSE NULL END)) account_name_glk, 
  concat_ws('/',collect_list(CASE WHEN t.type='ZZZJSKZH' THEN t.`name` ELSE NULL END )) account_name_ZZZJSKZH
  FROM `ods_bpms_biz_account` t WHERE t.`house_no`<>'' AND t.type IN ('GLK','ZZZJSKZH','QT(MMD)') 
  GROUP BY t.`house_no` 
),

t2 as (
  SELECT 
  t1.`apply_no`,
  min(t1.`fin_date`)  AS  floorAdvance_fin_date, -- 赎楼前垫资放款时间
  max(t2.`fin_date`) AS  floorAdvance_ret_date -- 赎楼前垫资归还时间
  FROM `ods_bpms_fd_advance` t1 
  LEFT JOIN `ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='floorAdvance' and t1.status <> "ZZ" 
  group by t1.`apply_no`
),

t3 as (
  SELECT 
  t1.`apply_no`,
  min(t1.`fin_date`)  AS expireAdvance_fin_date, -- 到期归还垫资时间
  max(t2.`fin_date`)  AS expireAdvance_ret_date -- 到期归还垫资归还时间
  FROM `ods_bpms_fd_advance` t1 
  LEFT JOIN `ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id` 
  WHERE t1.`adv_type`='expireAdvance' and  t1.status <> "ZZ"
  group by t1.`apply_no`
),

tmp_fd_advance_desc as (
  select 
    a.apply_no
    ,a.adv_amt expireAdvance_adv_amt -- 到期归还垫资金额
  from(
    select
    b.*, row_number() over(partition by b.apply_no order by b.id desc) rank
    from ods.ods_bpms_fd_advance b
    where  b.`adv_type`='expireAdvance' and  b.status <> "ZZ"
  ) as  a
  where rank = 1
),

tmp_fd_advance_fa_desc as (
  select 
    a.apply_no
    ,a.adv_amt floorAdvance_adv_amt -- 到期归还垫资金额
  from(
    select
    b.*, row_number() over(partition by b.apply_no order by b.id desc) rank
    from ods.ods_bpms_fd_advance b
    where  b.`adv_type`='floorAdvance' and  b.status <> "ZZ"
  ) as  a
  where rank = 1
),

con_t as (
  select 
  cct.apply_no
  , concat_ws('/',collect_list(cct.payee_acct)) payee_acct_jiaofei 
  , concat_ws('/',collect_list(CASE WHEN cct.`settl_way`='ZFB' THEN sd.NAME_ ELSE cct.payee_acct_no END ))  payee_acct_no_jiaofei
  , concat_ws('/',collect_list(cct.payee_bank_name)) payee_bank_name_jiaofei
   , concat_ws('/',collect_list(cct.payer_acct)) payer_acct_jiaofei -- 缴费出款账户
  , concat_ws('/',collect_list(CASE WHEN cct.`settl_way`='ZFB' THEN sd.NAME_ ELSE cct.payer_acct_no END ))  payer_acct_no_jiaofei -- 缴费出款账号
  , concat_ws('/',collect_list(cct.payee_bank_name)) payer_bank_name_jiaofei -- 缴费出款银行
  , concat_ws('/',collect_list(cast(cct.trans_day as string))) jiaofei_date 
  , SUM(cct.trans_money)  jiaofei_amt
  , COUNT(distinct settl_way) AS cnt
  , SUM(cct.premium) ss_premium_amount -- 实收保费
  from ods_bpms_c_cost_trade cct 
  left join ods_bpms_sys_dic sd on cct.`other_account` = sd.KEY_ and sd.TYPE_ID_ =  "10000056480061"
  where cct.trans_type = "CSC1"  
  group by cct.apply_no
),


con_d as (
  select 
  cct.apply_no
  , concat_ws('/',collect_list(cct.payee_acct)) payee_acct_tuifei 
  , concat_ws('/',collect_list(cct.payee_bank_name)) payee_bank_name_tuifei 
  , concat_ws('/',collect_list(CASE WHEN cct.`settl_way`='ZFB' THEN sd.NAME_ ELSE cct.payee_acct_no END )) payee_acct_no_tuifei
  , concat_ws('/',collect_list(CASE WHEN cct.`settl_way`='ZFB' THEN sd.NAME_ ELSE cct.payer_acct_no END )) payer_acct_no_tuifei
  , COUNT(distinct settl_way) cnt 
  , SUM(cct.trans_money) amount_tuifei
  , sum(cct.premium) tf_premium_amount -- 退费保费
  , concat_ws('/',collect_list(cast(cct.trans_money as string )) ) trans_money_tuifei
  , concat_ws('/',collect_list(cast(cct.trans_day as string))) trans_day_tuifei
  from ods_bpms_c_cost_trade cct 
  left join ods_bpms_sys_dic sd on cct.`other_account` = sd.KEY_ and sd.TYPE_ID_ =  "10000056480061"
  where cct.trans_type = "CSD1" and cct.trade_status = 1
  group by cct.apply_no
),


agg_biz_ledge as (
  select 
  bl.apply_no
  ,sum(bl.total_collection) total_collection
  ,max(bl.total_remark) total_remark
  ,sum(bl.assess_collect) assess_collect
  ,sum(bl.assess_pay)  assess_pay
  ,max(bl.assess_pay_time) assess_pay_time
  ,max(bl.assess_remark) assess_remark
  ,sum(bl.print_collect) print_collect
  ,sum(bl.print_pay) print_pay
  ,max(bl.print_pay_time) print_pay_time
  ,max(bl.print_pay_remark) print_pay_remark
  ,sum(bl.fair_collect) fair_collect
  ,sum(bl.fair_pay) fair_pay
  ,max(bl.fair_pay_time) fair_pay_time
  ,max(bl.fair_pay_remark) fair_pay_remark
  ,sum(bl.advance_penalty)  advance_penalty
  ,sum(bl.advance_collection) advance_collection
  ,max(bl.advance_colle_time) advance_colle_time
  ,max(bl.approval_compen_way) approval_compen_way
  ,sum(bl.approval_check) approval_check
  ,sum(bl.plat_money) plat_money
  ,max(bl.plat_check_day) plat_check_day
  ,sum(bl.plat_collection) plat_collection
  ,max(bl.plat_collect_day) plat_collect_day
  ,sum(bl.company_service) company_service
  ,sum(bl.company_rebate) company_rebate
  ,sum(bl.company_collect_rabate) company_collect_rabate
  ,sum(bl.company_collection) company_collection
  ,max(bl.company_remark) company_remark
  ,max(bl.insurance_is_collect) insurance_is_collect
  ,max(bl.insurance_remark) insurance_remark
  ,max(bl.insurance_rebate_remark) insurance_rebate_remark
  ,sum(bl.agency_channel) agency_channel -- 中介代收渠道费
  ,sum(bl.total_difference) total_difference -- 总收费差额
  from ods_bpms_biz_ledger bl 
  group by bl.apply_no
),

agg_biz_ledger_pay as (
  select 
  upper(bl.apply_no) apply_no
  ,sum(bl.humanpool_rebate) humanpool_rebate
  ,max(bl.humanpool_collect_day) humanpool_collect_day
  ,max(bl.humanpool_payment_day) humanpool_payment_day
  from ods_bpms_biz_ledger_pay bl 
  group by bl.apply_no
),

agg_biz_ledger_other as (
  select 
  trim(bl.apply_no) apply_no
  ,sum(bl.profits_money)  profits_money
  from ods_bpms_biz_ledger_other  bl
  group by trim(bl.apply_no)
),

agg_biz_fee_detial as (
  select 
  bfd.apply_no
  ,max(case when bfd.fee_define_no = "baseFeeFixedTerm" then bfd.fee_value end) baseFeeFixedTerm -- 基础收费-固定期限
  ,max(case when bfd.fee_define_no = "baseFeeCalculateDaily" then bfd.fee_value end) baseFeeCalculateDaily -- 基础收费-按天计息
  ,max(case when bfd.fee_define_no = "baseFee" then bfd.fee_value end) baseFee -- 基础收费
  ,max(case when bfd.fee_define_no = "rakebackFeeCalculateDaily" then bfd.fee_value end) rakebackFeeCalculateDaily -- 返佣费-按天计息
  ,max(case when bfd.fee_define_no = "rakebackFeeFixedTerm" then bfd.fee_value end) rakebackFeeFixedTerm -- 返佣费-固定期限
  ,max(case when lower(bfd.fee_define_no) in ("rakebackfee")  then bfd.fee_value end) rakeBackFee -- 返佣费
  ,max(case when bfd.fee_define_no in ('totalReceivableFee', 'feeTotal') then bfd.fee_value end) totalReceivableFee -- 应收合计
  ,sum(case when bfd.fee_name like '%偏远交通费%' then bfd.fee_value end ) pyjt_amount -- 偏远交通费
  ,max(case when bfd.fee_define_no = "evaluationServiceFee" then bfd.fee_value end) evaluationServiceFee -- 评估服务费
  ,sum(case when bfd.fee_define_no = "stampTaxFee" then bfd.fee_value end) stampTaxFee -- 印花税
  ,sum(case when bfd.fee_define_no = "notarizationFee" then bfd.fee_value end) notarizationFee -- 公正费 
  ,sum(case when bfd.fee_define_no = "baseServiceFee" then bfd.fee_value end) baseServiceFee -- 基础服务费
  ,sum(case when bfd.fee_define_no = "insurancePremium" then bfd.fee_value end) insurancePremium -- 保费 
  ,sum(case when bfd.fee_define_no = "nominalInsurancePremium" then bfd.fee_value end) nominalInsurancePremium -- 名义保费 
  ,sum(case when bfd.fee_define_no = "insuranceFee" then bfd.fee_value end) insuranceFee -- 保费
  ,sum(case when bfd.fee_define_no = "actualInsurancePremium" then bfd.fee_value end) actualInsurancePremium -- 实际保费
  ,sum(case when bfd.fee_define_no = "mortgageServiceFee" then bfd.fee_value end) mortgageServiceFee -- 按揭服务费
  ,sum(case when bfd.fee_name like '%抵押登记费%' then bfd.fee_value end) dydj_amount -- 抵押登记费
  ,sum(case when bfd.fee_name = "其他" then bfd.fee_value end) other_amount -- 其他
  from ods_bpms_biz_fee_detial bfd
  group by bfd.apply_no
),

agg_c_fund_module as (
  select 
  cfm.apply_no
  ,cfm.should_plat_cost 
  ,cfm.should_re_interest
  ,cfm.should_re_penalty
  ,cfm.con_payee_acct_name
  ,cfm.con_payee_bank
  ,cfm.con_payee_acct_no
  ,cfm.con_funds_time
  ,cfm.con_funds_cost
  ,cfm.fact_plat_cost
  ,cfm.fact_re_interest
  ,cfm.penalty_advanced_refund
  ,cfm.penalty_show_time
  ,cfm.fact_re_penalty
  ,cfm.fact_re_sum
  ,cfm.ad_pay_time
  from ods_bpms_c_fund_module_common cfm
),

agg_c_charge_detail as (
  select 
  trim(ccd.apply_no) apply_no
  ,sum(ccd.pre_collect)  reduction
  from ods_bpms_c_charge_detail ccd 
  where ccd.charge_type = "reduction" 
  group by trim(ccd.apply_no)
),

agg_biz_ledger_instead as (
  select 
  apply_no
  ,sum(bl.print_receivable) print_receivable
  ,sum(bl.assess_receivable) assess_receivable
  ,sum(bl.fair_receivable) fair_receivable
  ,sum(bl.replace_turn_out) replace_turn_out
  ,max(bl.replace_turn_day) replace_turn_day
  from ods_bpms_biz_ledger_instead bl 
  group by apply_no
),

agg_biz_ledger_invoice as (
  select 
  upper(apply_no) apply_no
  ,max(bl.invoice_submit_day) invoice_submit_day
  ,sum(bl.invoice_rebate) invoice_rebate
  ,max(bl.invoice_collect_day) invoice_collect_day
  from ods_bpms_biz_ledger_invoice bl 
  group by apply_no 
),


cct_cd01 as (
  select 
  apply_no
  ,max(cct.payer_acct) payer_acct
  ,max(cct.payer_bank_name) payer_bank_name
  ,max(cct.payer_acct_no) payer_acct_no
  ,max(cct.payee_acct) payee_acct
  ,max(cct.payee_bank_name) payee_bank_name
  ,max(cct.payee_acct_no) payee_acct_no
  from ods_bpms_c_cost_trade cct 
  where cct.trans_type = "CD01"  
  group by cct.apply_no 
),

cct_cc01 as (
  select 
  cct.apply_no
  ,concat_ws('/',collect_list(cct.payee_acct)) receive_name_hk 
  ,concat_ws('/',collect_list(cct.payee_bank_name)) receive_bank_hk
  ,concat_ws('/',collect_list(cct.payee_acct_no)) receive_acct_no_hk 
  ,concat_ws('/',collect_list(cast(cct.trans_day as string))) rebate_date_hk_all -- （客户）借款回款-回款日期
  ,max(cct.trans_day) rebate_date_hk -- （客户）借款回款-回款日期
  ,SUM(cct.`trans_money`) receive_amount_hk
  from ods_bpms_c_cost_trade cct 
  where cct.trans_type = "CC01" 
  group by cct.apply_no
),

cct_cc02 as (
  select 
  cct.apply_no
  ,max(cct.payer_acct) payer_acct
  ,max(cct.payer_bank_name) payer_bank_name
  ,max(cct.payer_acct_no) payer_acct_no
  ,max(cct.payee_acct) payee_acct
  ,max(cct.payee_bank_name) payee_bank_name
  ,max(cct.payee_acct_no) payee_acct_no
  ,max(cct.trans_day) trans_day
  from ods_bpms_c_cost_trade cct 
  where cct.trans_type = "CC02" 
  group by cct.apply_no
),

biz_channel as (
  select 
  apply_no
  ,bc.channel_name -- 渠道名称
  ,case when bc.channel_tag = "commonChannel" then "普通渠道"
        when bc.channel_tag = "specificChannel" then "特定渠道"
   end channel_tag -- 渠道标签
  from ods_bpms_biz_channel bc 
),

bomr as(
  select 
  apply_no
  ,max(case when bomr.matter_key = "financialArchiveEvent" then bomr.handle_time end) guidang_date -- 财务归档时间
  ,max(case when bomr.matter_key = "financialArchiveEvent" then bomr.handle_user_name end) guidang_user -- 财务归档用户
  ,min(case when bomr.matter_key = "Interview" then bomr.handle_time end) interview_date -- 面签时间
  ,max(case when bomr.matter_key = "paymentArrival" then bomr.handle_time end) paymentArrival -- 确认回款资金到账
  ,max(case when bomr.matter_key = "transferApply" then bomr.handle_time end) transferApply -- 资金划拨申请
  ,max(case when bomr.matter_key = "AgreeLoanMark" then bomr.handle_time end) AgreeLoanMark -- 同贷时间
  from ods_bpms_biz_order_matter_record bomr 
  where bomr.matter_key in ("financialArchiveEvent", "Interview", "InsuranceInfoConfirm", "paymentArrival", "transferApply", "")
  group by bomr.apply_no
),

bfdl as (
  select 
  apply_no 
  , sum(case when  (bfd.fee_define_no = 'channelPricePerOrder' or bfd.fee_define_no = 'channelPriceFixedTerm') and (bfd.fee_name not like "%按天计息%" or bfd.fee_name is null)
       then fee_value end) channel_price_per_order
  , sum(case when  (bfd.fee_define_no = "channelPriceTotal")
       then fee_value end) channel_price_total
  , sum(case when  (bfd.fee_define_no in ('channelPricePerDay', "channelPriceCalculateDaily") and bfd.fee_name not like '%固定期限%')
       then fee_value end) channel_price_per_day
  , sum(case when  (bfd.fee_define_no = "channelFeePerOrder")
       then fee_value end) channel_fee
  , sum(case when  (bfd.fee_define_no = "agentCommissionPerOrder")
       then fee_value end) agent_commission_per_order
  , sum(case when  (bfd.fee_define_no = "agentCommissionPerDay")
       then fee_value end) agent_commission_per_day
  , sum(case when  (bfd.fee_define_no = "agentCommissionFeePerOrder")
       then fee_value end) agent_commission_fee_per_order
  , sum(case when  (bfd.fee_define_no = "overdueRatePerDay")
       then fee_value end) overdue_rate_per_day
  , sum(case when  (bfd.fee_define_no in ('contractFeeRateTotal', "contractFeeRateCountCalculateDaily", "contractPriceCalculateDaily") and bfd.fee_name not like '%固定期限%')
       then fee_value end) contract_fee_rate_total_daily
  , sum(case when  (bfd.fee_define_no in ('contractFeeRateTotal', 'contractPriceFixedTerm', "contractFeeRateCountFixedTerm") and bfd.fee_name not like '%按天计息%')
       then fee_value end) contract_fee_rate_total_fix
  , sum(case when  (bfd.fee_define_no = "contractFeePerOrder")
       then fee_value end) contract_fee_per_order
  , sum(case when  (bfd.fee_define_no = "channelPricePerOrder")
       then fee_value end) channel_price_per_order_insr
  , sum(case when  (bfd.fee_define_no = "channelPrice")
       then fee_value end) channelPrice_insur
  , sum(case when  (bfd.fee_define_no = "contractPrice")
       then fee_value end) contract_price
  , sum(case when  (bfd.fee_define_no = "contractFeeRateTotal")
       then fee_value end) contract_fee_rate_total
  , SUM(CASE WHEN  (bfd.fee_define_no = "guaranteeFee")
       THEN fee_value END) guarantee_fee
  from ods_bpms_biz_fee_detial bfd
  group by apply_no
),


brf as (
  select 
  house_no
  ,sum(ransom_borrow_amount) ransom_borrow_amount -- 赎楼借款金额
  ,sum(ransom_cut_amount) ransom_cut_amount -- 赎楼扣款金额
  ,max(ransom_cut_time) ransom_cut_time -- 赎楼扣款时间
  from ods_bpms_biz_ransom_floor
  group by house_no
),

tmp_customer_all as 
(
  select 
  t1.apply_no
  ,concat_ws(',',collect_list(case when  t1.`role` IN ('OWN','SEL') then t2.`name` end)) seller_name_all -- 卖方：SEL
  ,concat_ws(',',collect_list(case when  t1.`role` IN ('BUY') then t2.`name` end)) buyer_name_all -- 买方：BUY
  from ods.ods_bpms_biz_customer_rel t1
  left join ods.ods_bpms_biz_customer t2 ON t1.`customer_no`=t2.`cust_no`
  where t1.`customer_no` <>''
  group by t1.apply_no
),


gl_tag as (
select 
bao.group_apply_no
,max(t100.NAME_) guanlian_type  -- 关联类型
from ods_bpms_biz_apply_order bao 
left join ods_bpms_sys_dic t100 on t100.type_id_='10000080670030' and bao.relate_type = t100.KEY_ 
where bao.relate_type <> 'MAIN' 
group by bao.group_apply_no
),

risk_tag as (
  select 
  a.apply_no
  ,case when risk_level = 'fbj' then '非标件'
        when a.risk_level = 'bzyw' then '标准业务'
  end risk_grade -- 风险标签
  from ods_bpms_biz_isr_mixed a 
),

account_rel as (
  select
  b.`partner_code` 
  ,b.insurance_code 
  ,b.area_code
  ,max(a.acct_name) acct_name
  ,max(a.bank_name) bank_name
  ,max(a.acct_no) acct_no
  from ods_bpms_b_account_info a
  left join ods_bpms_b_account_rel b on b.account_codes = a.id
  group by b.`partner_code`, b.insurance_code, b.area_code   
) ,

tmp_agg_1 as (
 select 
 bao.apply_no
 ,case when bfs.product_term_and_charge_way in ('N', 'fixedTerm') then '固定期限'
       when bfs.product_term_and_charge_way in ('Y', 'calculateDaily') then '按天计息'
       when bfs.product_term_and_charge_way = 'fixedAmount' then '固定金额'
       when bfs.product_term_and_charge_way = 'fixedRate' then '固定费率'
       when bfs.product_term_and_charge_way = 'piecewiseCalculate' then '区间计息'
  end  product_term_and_charge_way -- 收费方式
 from ods.ods_bpms_biz_apply_order bao 
 left join cct_cd01 on cct_cd01.apply_no = bao.apply_no
 left join agg_c_fund_module acfm on acfm.apply_no = bao.apply_no
 left join ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
 LEFT JOIN ods_bpms_biz_isr_mixed bim on bao.apply_no = bim.apply_no
),

tmp_biz_new_loan_h_d as (
  select 
  house_no
  ,biz_loan_amount
  from (
    select
    b.*, row_number() over(partition by b.house_no order by b.create_time desc) rank
    from ods.ods_bpms_biz_new_loan b
    where house_no <> "" and house_no is not null 
    ) as a
  where rank = 1
),

tmp_biz_new_loan_a_d as (
  select 
  apply_no
  ,biz_loan_amount
  ,new_loan_rate
  from (
    select
    b.*, row_number() over(partition by b.apply_no order by b.create_time desc) rank
    from ods.ods_bpms_biz_new_loan b
    where apply_no <> "" and apply_no is not null 
    ) as a
  where rank = 1
),

tmp_return_date as (
  select 
  a.apply_no
  ,nvl(b.trans_day, c.paymentArrival) return_date
  from ods.ods_bpms_biz_apply_order a
  left join cct_cc02 b on a.apply_no = b.apply_no
  left join bomr c on a.apply_no = c.apply_no
),

tmp_opinion as(
select
tmp.PROC_INST_ID_
,min(case when tmp.TASK_KEY_ = 'agreeLoanResult' then tmp.COMPLETE_TIME_ end) agreeLoanResult_time -- 确认银行放款时间
,min(case when tmp.TASK_KEY_ = 'agreeLoanResult' then tmp.AUDITOR_NAME_ end ) agreeLoanResult_user -- 确认银行放款经办人
from (
  select
    b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
    from ods_bpms_bpm_check_opinion b
    where b.COMPLETE_TIME_ is not null and b.TASK_KEY_ = 'agreeLoanResult'
) tmp
where rank = 1
group by tmp.PROC_INST_ID_
),

tmp_account_sl as (
  -- 若银行放款未及时到账，需从公司账户—借方赎楼户（垫资）
  SELECT t.`house_no`
  ,concat_ws('/',collect_list(t.`name`)) account_name -- 划入户名
  ,concat_ws('/',collect_list(t.`open_bank`)) open_bank -- 划入银行
  ,concat_ws('/',collect_list(t.`number`)) `number` -- 划入账号


  FROM ods.`ods_bpms_biz_account` t 
  WHERE t.`house_no`<>'' 
  GROUP BY t.`house_no` 
  ),

tmp_insurance_policy as (
select apply_no,
insurance_id, -- 投保ID
policy_no, -- 保单号码
policy_start,  --  保险起期
policy_end, --  保险止期
loan_amount, -- 借款总金额
premium_amount,-- 保费
insurance_amount,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_insurance_policy
),

tmp_ledger_instead as (
  select 
  apply_no
  ,sum(mortgage_collect) mortgage_collect -- 代收代付：抵押登记费实收
  ,sum(mortgage_receivable) mortgage_receivable -- '代收代付：抵押登记费应收'
  ,sum(mortgage_pay) mortgage_pay -- '代收代付：抵押登记费支付金额'
  ,max(mortgage_pay_time) mortgage_pay_time -- '代收代付：抵押登记费支付时间'
  ,concat_ws('/',collect_list(mortgage_pay_remark)) mortgage_pay_remark
  from ods.ods_bpms_biz_ledger_instead 
  group by apply_no
),

tmp_loan_time_yy as (
  select 
  bao.apply_no
  ,case when instr(bao.product_name,'大道按揭')>0 
        then bffi.free_mortgage_time 
       else oofc.loan_time_xg 
  end loan_time_yy -- 放款时间_运营
  from ods.ods_bpms_biz_apply_order_common bao
  left join ods.ods_bpms_biz_fund_flow_info bffi on bao.apply_no = bffi.apply_no
  left join ods.ods_orders_finance_common oofc on bao.apply_no = oofc.apply_no
),
tmp_fee_value as (
  select 
  bfd.apply_no
  ,max(case when bbfm.fee_metadata_name = "渠道价（保险）/笔（%）" then bfd.fee_value end)  qdj_insur_b
  ,max(case when bbfm.fee_metadata_name = "保费分成比例（%）" then bfd.fee_value end)  bffc_bl
  from ods.ods_bpms_biz_base_fee_metadata bbfm
  inner join ods.ods_bpms_biz_fee_detial bfd on bfd.fee_define_no = bbfm.fee_metadata_code
  where (bbfm.fee_metadata_name = "渠道价（保险）/笔（%）" or bbfm.fee_metadata_name = "保费分成比例（%）") 
  group by  bfd.apply_no 
),
tmp_fund_flow_info as (
select apply_no,
funded_repayment_time,   --  逾期垫资还款时间
funded_repayment_amount, --  逾期垫资还款金额
return_funded_time,      --  归还平台借款时间
b.fullname_ paymensave_user_name, -- 确认回款保存经办人
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_fund_flow_info a
left join ods_bpms_sys_user b on b.id_=a.update_user_id

)

insert overwrite table dwd.`tmp_dwd_order_finance`
select *
,nvl(print_receivable,0) + nvl(mortgage_receivable,0) + nvl(assess_receivable,0) + nvl(fair_receivable,0) AS company_collect_rabate --'大道服务费-代收代付'
,nvl(actual_amount_total,0) + nvl(ss_premium_amount,0) AS customer_fee_total -- '客户收费总额'
,nvl(actual_amount_total,0) - nvl(zsf_ys,0) AS differ_total -- '总收费-差额'
,nvl(zsf_ys,0) - nvl(should_plat_cost,0) - nvl(ought_amount_plat_interest,0) - nvl(guarantee_fee,0) AS ddfwf_ys -- '大道服务费-应收'
,nvl(actual_amount_total,0) - nvl(should_plat_cost,0) - nvl(ought_amount_plat_interest,0) AS company_collection -- '大道服务费-实收'
,nvl(plat_money,0) - nvl(should_plat_cost,0) AS compare_amount -- '平台费对账-差额'
,nvl(should_plat_cost,0) - nvl(loan_interest,0) - nvl(early_repay_faxi,0) AS check_02 -- '借款归还-check'
from( 
select
bao.apply_no
,case when bao.product_name = "及时贷（非交易））" then "及时贷（非交易提放）"
       when bao.product_name = "提放保-无赎楼" then "提放保（无赎楼）"
       when bao.product_name = "提放保-有赎楼" then "提放保（有赎楼）"
       when bao.product_name = "提放保(无赎楼)" then "提放保（无赎楼）"
  else bao.product_name
 end product_name 

,case when bao.product_type = "现金类产品" then 
       (case 
            -- 系统显示已经退单（ 终止原因为客户退单）
            when  bao.apply_status = "finished" 
            and (bao.remark like "%客户退单%" or  bao.remark like "%进件渠道退单%") then "退单" 

            when bao.apply_status = "finished"    then "终止"

            -- 客户回款且已经回平台款
            when cct_cc01.rebate_date_hk is not null  and cct_cc02.trans_day is not null  then "完结"

            -- 财务归档事项已提交
            when if(bfs.fund_package_code is not null  and bfs.fund_package_code <> "ZY" and bfs.fund_package_code <> "JM", "是", "否") = "是"
                 and cct_cc01.rebate_date_hk is not null  then "财务归档"
            -- when bomr.guidang_date is not null then "财务归档"
            
            -- 赎楼前垫资且平台未放款
            when (CASE WHEN t2.apply_no IS NOT NULL THEN '是' ELSE '否' END ) = "是" 
                 and (bfs.platform_value_date is null or bfs.platform_value_date = "" ) then "赎楼前垫资"
           
            -- 平台未放款
            when (bfs.platform_value_date is null or bfs.platform_value_date = "" )  then "平台未放款"

            -- 平台已放款且无到期归还垫资且客户未回款
            when bfs.platform_value_date is not null and  bfs.platform_value_date <> ""
                 and (CASE WHEN t3.apply_no IS NOT NULL THEN '是' ELSE '否' END ) = "否" and cct_cc01.rebate_date_hk is null then "平台已放款"  
            
            -- 到期归还垫资且已归还平台款
            when cct_cc02.trans_day is not null 
                 and (CASE WHEN t3.apply_no IS NOT NULL THEN '是' ELSE '否' END ) = "是"  then "回款垫资" 

            -- 客户已回款
            when cct_cc01.rebate_date_hk is not null then "客户回款" 
        end)

    when bao.product_type = "保险类产品" then 
      (case when bao.apply_status = "finished" and (bao.remark like "%客户退单%" or  bao.remark like "%进件渠道退单%") then "退单" 
            when bao.apply_status = "finished" then "终止"
            when bomr.guidang_date is not null then "财务归档"
            else '正常'
       end)
    
    else 
      (case 
          -- 系统显示已经退单（ 终止原因为客户退单）
          when bao.apply_status = "finished" and (bao.remark like "%客户退单%" or  bao.remark like "%进件渠道退单%") then "退单" 
          when bao.apply_status = "finished" then "终止"
          -- 财务归档事项已提交
          when bomr.guidang_date is not null then "财务归档"
          else "正常"
       end)

end AS apply_status  -- 订单状态
,bao.partner_insurance_name -- 合作机构、合作保险
,bao.partner_bank_name -- 合作银行
,bfs.product_term -- 借款期限
,bao.sales_user_name -- 渠道经理
,if(bnl_a.biz_loan_amount > 0, bnl_a.biz_loan_amount, bnl_h.biz_loan_amount) biz_loan_amount -- 买家按揭
,bomr.interview_date -- 面签时间
,case when bao.product_type='现金类产品' then cast(bfs.borrowing_amount as string)
      when bao.product_type='保险类产品' then nvl(cast(bfs.ransom_borrow_amount as string), cast(brf_z.ransom_borrow_amount as string))
      else NULL 
 end shenqing_amount   -- 申请金额
,case when bfs.product_term_and_charge_way in ('N', 'fixedTerm') then '固定期限'
       when bfs.product_term_and_charge_way in ('Y', 'calculateDaily') then '按天计息'
       when bfs.product_term_and_charge_way = 'fixedAmount' then '固定金额'
       when bfs.product_term_and_charge_way = 'fixedRate' then '固定费率'
       when bfs.product_term_and_charge_way = 'piecewiseCalculate' then '区间计息'
  end  product_term_and_charge_way -- 收费方式
,bfs.fixed_term -- 客户收费期限
,bao.relate_type_name guanlian_type  -- 关联类型
-- 主订单没有关联id 
,case when bao.relate_type not in ('MAIN', 'main') then bao.group_apply_no 
end group_apply_no -- 关联id  

,rkt.risk_grade -- 风险标签
,nvl(pet.NAME_, bfs.`price_tag`) price_tag -- 价格标签
,bc.channel_tag -- 渠道标签

,case when trd.return_date is null then datediff(nvl(to_date(regexp_replace(cast(trd.return_date as string), "/", "-")), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(bfs.borrowing_value_date as string), "/", "-"))+1   
      when trd.return_date is not null then datediff(nvl(to_date(regexp_replace(cast(trd.return_date as string), "/", "-")), date_sub(from_unixtime(unix_timestamp(),'yyyy-MM-dd'), 1)), regexp_replace(cast(bfs.borrowing_value_date as string), "/", "-"))
end use_days -- 实际借款期限

,case when bfs.product_term_and_charge_way in ('N', 'fixedTerm', 'fixedRate') 
       then nvl(bfdl.channel_price_per_order, 0) 
       
       when bfs.product_term_and_charge_way = 'piecewiseCalculate'
       then nvl(bfdl.channel_price_total, 0)

       when bfs.product_term_and_charge_way in ('Y', 'calculateDaily') 
       then nvl(bfdl.channel_price_per_day, 0)

       when bfs.product_term_and_charge_way = 'fixedAmount' 
       then nvl(bfdl.channel_fee, 0)

       else nvl(bfdl.channel_price_per_order_insr, bfdl.channelPrice_insur)
  end  channel_price -- 渠道价费率

,(case when bfs.product_term_and_charge_way in ('N', 'fixedTerm', 'fixedRate', 'piecewiseCalculate') 
       then nvl(bfdl.agent_commission_per_order, 0)

       when bfs.product_term_and_charge_way in ('Y', 'calculateDaily') 
       then nvl(bfdl.agent_commission_per_day, 0) 

       when bfs.product_term_and_charge_way = 'fixedAmount' 
       then nvl(bfdl.agent_commission_fee_per_order, 0)

       else nvl(bfdl.agent_commission_per_order, 0)
  end ) agent_commission -- 代收返佣费率

,(case when bfs.product_term_and_charge_way in ('N', 'fixedTerm', 'piecewiseCalculate') 
       then  nvl(bfdl.overdue_rate_per_day, 0) 
  end ) overdue_rate -- 超期费率

,(case when bfs.product_term_and_charge_way in ('Y', 'calculateDaily') 
       then nvl(bfdl.contract_fee_rate_total_daily, 0)

       when bfs.product_term_and_charge_way in ( 'N', 'fixedTerm', 'fixedRate', 'piecewiseCalculate') 
       then nvl(bfdl.contract_fee_rate_total_fix, 0)

       when bfs.product_term_and_charge_way = "fixedAmount"
       then nvl(bfdl.contract_fee_per_order, 0)

       else nvl(bfdl.contract_fee_rate_total, bfdl.contract_price)
   end) htflhj -- 合同费率合计

,DATE_ADD(regexp_replace(bfs.borrowing_value_date, "/", "-"), cast(bfs.`fixed_term` as int)) AS borrowing_due_date_cust -- 借款到期日-客户收费
,DATE_ADD(regexp_replace(bfs.platform_value_date, "/", "-"),  cast(bfs.`product_term` as int )) AS borrowing_due_date_platform -- 借款到期日-平台发标

,con_t.payee_acct_jiaofei -- 服务费-缴费-收款户名
,con_t.payee_bank_name_jiaofei -- 服务费-缴费-收款银行
,con_t.payee_acct_no_jiaofei -- 服务费-缴费-收款帐号
,con_t.jiaofei_date -- 服务费-缴费-转入日期(非日期格式)
,con_t.jiaofei_amt -- 服务费-缴费-转入金额
,con_d.payee_acct_tuifei -- 服务费-退费-收款户名
,con_d.payee_bank_name_tuifei --服务费-退费-收款银行
,con_d.payee_acct_no_tuifei -- 服务费-退费-收款帐号
,con_d.trans_day_tuifei -- 服务费-退费-转出日期(非日期格式)
,con_d.amount_tuifei trans_money_tuifei -- 服务费-退费-转出金额
,nvl(con_t.jiaofei_amt, 0) - nvl(con_d.amount_tuifei, 0)  actual_amount_total -- 总收费-实收

,nvl(abfd.totalReceivableFee, 0) 
- nvl(accd.reduction, 0)   AS zsf_ys -- 总收费-应收
,nvl(accd.reduction, 0) reduction_amount --总收费-减免金额

--,nvl(abl.total_difference, 0) differ_total -- 总收费-差额
,abl.total_remark -- 总收费-备注

,abfd.pyjt_amount --公司服务费-其中：偏远交通费
,nvl(abfd.mortgageServiceFee, 0) gsfwf_ajfwf -- 公司服务费-其中：按揭服务费
,abl.company_service --'大道服务费-其中：服务费（元）',
,abfd.pyjt_amount   `ddfwf_fjfwf` --'大道服务费-其中：附加服务费',
,abl.company_rebate -- '大道服务费-其中：汇思返佣+发票',
,abfd.rakebackFee qzqdfy_amount  --'大道服务费-应返返佣金额',
,nvl(abl.company_collection, 0) - nvl(abfd.baseServiceFee,0) ddfwf_check --'大道服务费-Check',
,abl.company_remark --'大道服务费-备注',
,nvl(abl.company_collection, 0) - nvl(abl.company_rebate, 0) - nvl(abl.company_collection, 0) ddfwf_ce --大道服务费-差额(实际服务费）'
,nvl(abfd.nominalInsurancePremium, nvl(abfd.insurancePremium, abfd.insuranceFee)) premium_amount   -- '保费-应收（元）',
,abl.insurance_is_collect insurance_is_collect  -- '保费-是否代收',
,abl.insurance_remark -- '保费-备注',
,(CASE WHEN bao.partner_insurance_name LIKE '%众安%' THEN  nvl(brf.ransom_borrow_amount, bfs.ransom_borrow_amount)*0.0018  ELSE 0 END ) AS actual_amount_rebate -- '保险公司返点-应收金额（元）',
,abl.insurance_rebate_remark -- '保险公司返点-备注'
,acfm.should_plat_cost -- 平台服务费-应收
,nvl(acfm.should_re_interest, 0) + nvl(acfm.should_re_penalty, 0) AS ought_amount_plat_interest --平台利息-应收
,abfd.evaluationServiceFee assess_receivable --代收代付：评估费-应收（元）

,abl.assess_collect -- 代收代付：评估费-实收
,abl.assess_pay --代收代付：评估费-支付金额
,abl.assess_pay_time -- 代收代付：评估费-支付时间
,abl.assess_remark  -- 代收代付：评估费 - 备注

,abfd.stampTaxFee print_receivable  -- 代收代付：印花税 - 应收（元）
,abl.print_collect -- 代收代付：印花税 - 实收
,abl.print_pay -- 代收代付：印花税 - 支付金额
,abl.print_pay_time -- 代收代付：印花税 -支付时间
,abl.print_pay_remark -- 代收代付：印花税 - 备注

,abfd.notarizationFee fair_receivable  -- 代收代付：公证费 - 应收（元）
,abl.fair_collect -- 代收代付：公证费 - 实收
,abl.fair_pay -- 代收代付：公证费 - 支付金额
,abl.fair_pay_time -- 代收代付：公证费 -支付时间
,abl.fair_pay_remark  -- 代收代付：公证费 - 备注

,case when bao.product_name like '%及时贷%' then acfm.con_payee_acct_name 
 else cct_cd01.payee_acct
 end receive_name_plat_fk -- 放款（到赎楼共管户）-收款户名
,case when bao.product_name like '%及时贷%' then acfm.con_payee_bank
 else cct_cd01.payee_bank_name 
 end  receive_bank_plat_fk -- 放款（到赎楼共管户）-收款银行

,case when bao.product_name like '%及时贷%' then acfm.con_payee_acct_no 
 else case 
      when bao.product_name like "%买付保%" and bfs.random_pay_mode in ('buyerPay', 'companyHelpPay') then null
      else cct_cd01.payee_acct_no end
end receive_acct_no_plat_fk -- 放款（到赎楼共管户）-收款帐号
,(CASE 
  WHEN bao.product_name LIKE '%赎楼E%' AND t1.account_name_glk IS NOT NULL THEN  bfs.borrowing_amount
  WHEN bao.product_name LIKE '%及时贷%' AND t1.account_name_ZZZJSKZH IS NOT NULL THEN  bfs.borrowing_amount 
  when bao.product_name Like "%买付保%" and bfs.random_pay_mode = 'buyerPay' then null 
  else nvl(bim.bank_loan_amount, bim.apply_loan_amount)
  END ) AS realease_amount -- 放款（到赎楼共管户）-到帐金额
,cct_cd01.payer_acct out_name_hb  --资金划拨-出款户名
,cct_cd01.payer_bank_name out_bank_hb  -- 资金划拨-出款银行
,cct_cd01.payer_acct_no out_acct_no_hb --资金划拨-出款帐号 
,cct_cd01.payee_acct receive_name_hb --资金划拨-收款户名
,cct_cd01.payee_bank_name receive_bank_hb  -- 资金划拨-收款银行
,cct_cd01.payee_acct_no receive_acct_no_hb --资金划拨-收款账号

,case when bao.product_type = "保险类产品" 
      then (CASE WHEN bao.`product_name` LIKE '%有赎楼%' AND bomr.transferApply IS NOT NULL THEN bomr.transferApply END )
     else (CASE WHEN cct_cd01.payer_acct IS NOT NULL THEN acfm.con_funds_time ELSE NULL END ) 
end huabo_date --资金划拨-划出日期

,case when bao.product_type = "保险类产品" 
      then (CASE WHEN bao.`product_name` LIKE '%有赎楼%' AND bomr.transferApply IS NOT NULL THEN nvl(bfs.`ransom_borrow_amount`, brf.`ransom_borrow_amount`) END )
     else (CASE WHEN cct_cd01.payer_acct IS NOT NULL THEN acfm.con_funds_cost ELSE NULL END)
end huabo_amount --资金划拨-划出金额
,CASE WHEN bao.product_name LIKE '%有赎楼%' THEN bim.tail_start_time else null end sfwk_date -- 赎楼信息-释放尾款时间

,(CASE WHEN t2.apply_no IS NOT NULL THEN '是' ELSE '否' END )  AS advance_before_shulou -- 垫资信息-是否赎楼前垫资
,tfafd.floorAdvance_adv_amt --垫资信息-赎楼前垫资金额
,t2.floorAdvance_fin_date -- 垫资信息-赎楼前垫资出款时间
,t2.floorAdvance_ret_date -- 垫资信息-赎楼前垫资回款时间
,(CASE WHEN t3.apply_no IS NOT NULL THEN '是' ELSE '否' END )  AS advance_return -- 垫资信息-是否到期归还垫资
,tfad.expireAdvance_adv_amt -- 垫资信息-回款垫资金额
,t3.expireAdvance_fin_date -- 垫资信息-回款垫资出款时间
,t3.expireAdvance_ret_date -- 垫资信息-回款垫资回款时间

,(case 
  when DATEDIFF(regexp_replace(t3.expireAdvance_ret_date, "/", "-"), regexp_replace(t3.expireAdvance_fin_date, "/", "-")) in ('0','1') then 0 
  else DATEDIFF(regexp_replace(t3.expireAdvance_ret_date, "/", "-"), regexp_replace(t3.expireAdvance_fin_date, "/", "-")) 
  end ) advance_day -- 到期垫资罚息收取信息-垫资天数

,abl.advance_penalty -- 到期垫资罚息收取信息-罚息率/天
,abl.advance_penalty * DATEDIFF(regexp_replace(t2.floorAdvance_ret_date, "/", "-"), regexp_replace(t2.floorAdvance_fin_date, "/", "-")) AS yingshoufaxijine -- 到期垫资罚息收取信息-应收罚息金额（元）

,abl.advance_collection  -- 到期垫资罚息收取信息-实收罚息金额（元）
,abl.advance_colle_time --到期垫资罚息收取信息-实收罚息时间
,cct_cc01.receive_name_hk --（客户）借款回款-收款户名
,cct_cc01.receive_bank_hk --（客户）借款回款-收款银行
,cct_cc01.receive_acct_no_hk --（客户）借款回款-收款帐号
,cct_cc01.receive_amount_hk --（客户）借款回款-收款金额（元）
,cct_cc01.rebate_date_hk_all  --（客户）借款回款-回款日期
,cct_cc02.payer_acct out_name -- 借款归还-出款户名
,cct_cc02.payer_bank_name cc02_out_bank_hb -- 借款归还-出款银行
,cct_cc02.payer_acct_no cc02_out_acct_no_hb --借款归还-出款账号
,cct_cc02.payee_acct cc02_receive_name_guihuan -- 借款归还-收款户名
,cct_cc02.payee_bank_name cc02_receive_bank_guihuan -- 借款归还-收款银行
,cct_cc02.payee_acct_no cc02_receive_acct_no_guihuan -- 借款归还-收款帐号

,acfm.fact_plat_cost  fact_plat_cost -- 借款归还-平台费（元）
,acfm.fact_re_interest loan_interest --借款归还-借款利息（元）
,acfm.fact_re_penalty early_repay_faxi --借款归还-提前还款罚息（元）

,nvl(acfm.fact_plat_cost, 0) - nvl(acfm.should_plat_cost, 0) AS check_01 -- 借款归还-check

,acfm.fact_re_sum return_amount -- 借款归还-归还金额（元）
,cct_cc02.trans_day return_date -- 借款归还-归还日期
,bc.channel_name --审批及报销方式-渠道公司
,abl.approval_compen_way --审批及报销方式-报销方式
,abl.approval_check --审批及报销方式-CHECK
,ablp.humanpool_rebate  -- 汇思支付信息-返佣金额（元）
,ablp.humanpool_collect_day  -- 汇思支付信息-实际付款日期
,ablin.invoice_rebate -- 发票报销信息-返佣金额（元）f
,ablin.invoice_submit_day -- 发票报销信息-报销单提交总部日期
,ablin.invoice_collect_day -- 发票报销信息-实际付款日期
,abli.replace_turn_out --代收代付返佣-转出金额
,abli.replace_turn_day -- 代收代付返佣-转出日期
,abl.plat_money --平台费对账-平台金额（元）
--,nvl(abl.plat_money, 0) - nvl(acfm.should_plat_cost, 0) AS compare_amount --平台费对账-差额
,abl.plat_check_day --平台费对账-对账日期 
,abl.plat_collection --平台费对账-实际付款金额（元）
,abl.plat_collect_day --平台费对账-实际付款日期
,bomr.guidang_date  --财务归档时间
,abl.agency_channel -- 中介代收渠道费
,bomr.guidang_user  --财务归档处理人
,bfs.borrowing_value_date -- 借款起息日
,if(bao.service_type = "JMS", "是", "否") is_jms -- 是否加盟商业务
,bfs.platform_value_date -- 平台起息日
,ablo.profits_money -- 分润金额
,dca.company_name_2_level city_name-- 分公司
,dca.company_name_3_level branch_name -- 附属公司
,bao.apply_time -- 订单申请时间
,bao.product_type -- 产品类型
,bim.bank_loan_time -- 银行放款时间
,bim.insurance_release_time -- 保险责任解除时间
,bkfp.fund_package_name is_fund_package -- 是否资金包
,abfd.other_amount -- 公司服务费-其他金额
,bfs.price_discount -- 定价折扣
,tcall.seller_name_all -- 卖方所有人员姓名
,tcall.buyer_name_all -- 买方所有人员姓名

,bim.tail_release_node_name -- 放款节点
,tip1.policy_no --  保单号码
,tip1.premium_amount dd_2_insur_premium_amount  --  实缴保费金额 ， 大道给保险公司的保费
,nvl(con_t.ss_premium_amount, 0) - nvl(con_d.tf_premium_amount, 0) ss_premium_amount -- 实收保费
,case when tip1.policy_no is not null or tip1.policy_no <> '' then '是'
      else '否'
 end is_create_policy_no  -- 是否出保单
,tli.mortgage_collect -- 代收代付：抵押登记费实收
,abfd.dydj_amount mortgage_receivable -- '代收代付：抵押登记费应收
,tli.mortgage_pay -- '代收代付：抵押登记费支付金额'
,tli.mortgage_pay_time -- '代收代付：抵押登记费支付时间'
,tli.mortgage_pay_remark -- 代收代付：抵押登记费备注
,case when bao.product_type = '现金类产品' then cast(bfs.platform_value_date as timestamp)-- 借款起息
 else tltyy.loan_time_yy
 end loan_time_jc -- 到账时间_计财
,oofc.loan_time_xg  -- 放款时间_销管
,con_t.payer_acct_jiaofei -- 服务费-缴费-出款户名
,con_t.payer_acct_no_jiaofei -- 服务费-缴费-出款账户
,con_t.payer_bank_name_jiaofei -- 服务费-缴费-出款银行
,con_d.payer_acct_no_tuifei -- 服务费-退费-出款账户
,ablp.humanpool_payment_day -- 汇思支付-提交总部日期
,abfd.actualInsurancePremium -- 保费-应收(实际)
,case when baoe.profit_exceed > 0 then baoe.profit_exceed end profit_exceed -- 校验差额
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
,if(baoe.profit_exceed > 0 , baoe.financial_time, null) financial_time -- 校验差额时间
,bfdl.guarantee_fee -- 担保费
,IF(cct_cc02.trans_day IS NOT NULL AND bfdl.guarantee_fee >0 AND trade_cc01.total_cc01_money>= bfs.borrowing_amount,
    bfdl.guarantee_fee-bfs.borrowing_amount*DATEDIFF(cct_cc01.rebate_date_hk,bfs.borrowing_value_date)*0.003/360,0) guarantee_fee_return -- 担保费-预估返还
,trade_cc01.max_cc01_time max_payment_time -- （客户）最新回款日
,tfv.qdj_insur_b -- 渠道价（保险）/笔（%）
,tfv.bffc_bl -- 保费分成比例（%）
,acfm.ad_pay_time  --  赎楼前垫资回款时间
,tfi.funded_repayment_amount --  逾期垫资还款金额
,dwx.financingcostrateperday_fee_value -- 担保费/天（%）
,dwx.guaranteefeerateperday_fee_value  -- 融资成本率/天（%）
,9.99 -- 贷款利率
from ods_bpms_biz_apply_order_common bao
LEFT JOIN ods_bpms_biz_isr_mixed_common bim on bao.apply_no = bim.apply_no
left join ods_bpms_biz_fee_summary bfs on bao.apply_no = bfs.apply_no
left join tmp_biz_new_loan_a_d bnl_a on bao.apply_no = bnl_a.apply_no 
left join tmp_biz_new_loan_h_d bnl_h on bao.house_no = bnl_h.house_no
left join bfdl on bao.apply_no = bfdl.apply_no 
left join con_t on con_t.apply_no = bao.apply_no
left join con_d on con_d.apply_no = bao.apply_no
left join agg_biz_ledge abl on abl.apply_no = bao.apply_no
left join agg_biz_fee_detial abfd on abfd.apply_no = bao.apply_no
left join agg_c_fund_module acfm on acfm.apply_no = bao.apply_no
left join agg_c_charge_detail accd on accd.apply_no = bao.apply_no
left join agg_biz_ledger_instead abli on abli.apply_no = bao.apply_no
left join cct_cd01 on cct_cd01.apply_no = bao.apply_no
left join agg_biz_ledger_pay ablp on ablp.apply_no = bao.apply_no
left join biz_channel bc on bc.apply_no = bao.apply_no
left join bomr on bomr.apply_no = bao.apply_no
left join t1 on t1.house_no = bao.house_no
left join t2 on t2.apply_no = bao.apply_no
left join t3 on t3.apply_no = bao.apply_no
left join cct_cc01 on cct_cc01.apply_no = bao.apply_no
left join cct_cc02 on cct_cc02.apply_no = bao.apply_no
left join agg_biz_ledger_invoice ablin on ablin.apply_no = bao.apply_no
left join brf on brf.house_no = bao.house_no
LEFT JOIN ods_bpms_sys_org so_o ON (bao.`branch_id`=so_o.`CODE_`) 
left join gl_tag glt on glt.group_apply_no = bao.apply_no
left join risk_tag rkt on bao.apply_no = rkt.apply_no
left join ods_bpms_sys_dic pet on (bfs.`price_tag`=pet.KEY_ and pet.`TYPE_ID_`='10000042640043')
left join agg_biz_ledger_other ablo on bao.apply_no = ablo.apply_no
left join tmp_agg_1 ta1 on ta1.apply_no = bao.apply_no
left join tmp_fd_advance_desc tfad on tfad.apply_no = bao.apply_no
left join tmp_fd_advance_fa_desc tfafd on tfafd.apply_no = bao.apply_no
left join tmp_return_date trd on trd.apply_no = bao.apply_no
left join tmp_opinion opi on bao.flow_instance_id = opi.PROC_INST_ID_
left join tmp_account_sl tasl on bao.house_no = tasl.house_no
left join dim.dim_company dca on bao.branch_id = dca.company_id_3_level
left join tmp_customer_all tcall on bao.apply_no = tcall.apply_no
left join (select * from ods_bpms_biz_ransom_floor_common where rn=1 ) brf_z on bao.apply_no = brf_z.apply_no
left join (select * from tmp_insurance_policy tip where rn=1 )  tip1 on tip1.apply_no=bao.apply_no
left join tmp_ledger_instead tli on bao.apply_no = tli.apply_no
left join tmp_loan_time_yy tltyy on bao.apply_no = tltyy.apply_no
left join ods.ods_orders_finance_common oofc on bao.apply_no = oofc.apply_no
left join ods.ods_bpms_biz_apply_order_extend baoe on bao.apply_no = baoe.apply_no
LEFT JOIN (SELECT obcct.apply_no, MAX(obcct.trans_day) max_cc01_time,MAX(b.fullname_) max_fullname, SUM(obcct.trans_money) total_cc01_money
	FROM ods.ods_bpms_c_cost_trade obcct LEFT JOIN ods.ods_bpms_sys_user b ON (b.id_=obcct.update_user_id)
	WHERE obcct.trans_type = "CC01" GROUP BY obcct.apply_no) trade_cc01 ON trade_cc01.apply_no = bao.apply_no
left join ods.ods_bpms_biz_lm_fund_package bkfp on bfs.fund_package_code = bkfp.fund_package_code
left join tmp_fee_value tfv on bao.apply_no = tfv.apply_no
left join (select * from tmp_fund_flow_info where rn=1) tfi on tfi.apply_no=bao.apply_no
left join ods_bpms_order_fees_common_wx dwx on dwx.apply_no = bao.apply_no
) t;