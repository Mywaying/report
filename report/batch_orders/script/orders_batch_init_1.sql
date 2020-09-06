set hive.execution.engine=spark;
use ods ;
drop table if exists dwd.dwdtmp_orders_ex;
create table if not exists dwd.dwdtmp_orders_ex (
  apply_no	STRING	COMMENT	'订单编号',
  product_version	STRING	COMMENT	'版本',
  product_name	STRING	COMMENT	'产品名称',
  product_type	STRING	COMMENT	'产品类型',
  transaction_type	STRING	COMMENT	'交易类型',
  order_amount	DOUBLE	COMMENT	'缴/退费金额',
  apply_status_name	STRING	COMMENT	'订单状态',
  group_apply_no	STRING	COMMENT	'主关联订单',
  relate_type_new	STRING	COMMENT	'关联生成关系',
  relate_type_name	STRING	COMMENT	'关联类型',
  risk_grade	STRING	COMMENT	'风险等级',
  price_tag	STRING	COMMENT	'价格标签',
  tail_release_node	STRING	COMMENT	'放款节点',
  apply_time	TIMESTAMP	COMMENT	'业务申请时间',
  order_update_time	TIMESTAMP	COMMENT	'订单更新时间',
  org_name	STRING	COMMENT	'所属团队',
  org_leader	STRING	COMMENT	'团队长',
  channel_name	STRING	COMMENT	'渠道名称',
  channel_type_name	STRING	COMMENT	'渠道类型',
  channel_tag_new	STRING	COMMENT	'渠道标签',
  sales_user_name	STRING	COMMENT	'渠道经理',
  channel_phone	STRING	COMMENT	'渠道联系人电话',
  channel_rebate_man	STRING	COMMENT	'返佣人',
  channel_rebate_bank	STRING	COMMENT	'返佣开户行',
  channel_rebate_card_no	STRING	COMMENT	'返佣账号',
  partner_insurance_name	STRING	COMMENT	'合作机构/保险',
  partner_bank_name	STRING	COMMENT	'合作银行',
  own_fund_amount	DOUBLE	COMMENT	'卖方自有资金',
  random_pay_mode	STRING	COMMENT	'赎楼款支付方式',
  tail_pay_mode	STRING	COMMENT	'尾款支付方式',
  transfertail_time	TIMESTAMP	COMMENT	'尾款划拨申请时间',
  transfertail_user_name	STRING	COMMENT	'尾款划拨申请经办人',
  transfertail_amount	STRING	COMMENT	'尾款划拨申请金额',
  confirmtransfertail_time	TIMESTAMP	COMMENT	'尾款划拨确认时间',
  confirmtransfertail_user_name	STRING	COMMENT	'尾款划拨确认经办人',
  confirmtransfertail_amount	STRING	COMMENT	'尾款划拨确认金额',
  confirmtailarrial_time	TIMESTAMP	COMMENT	'确认资金到账(尾款)时间',
  fixed_term	bigint	comment	'预估用款期限',
  seller_name	STRING	COMMENT	'卖方/产权人姓名',
  seller_id_card_no	STRING	COMMENT	'卖方/产权人证件号',
  seller_phone	STRING	COMMENT	'卖方/产权人联系方式',
  seller_income_type_name	STRING	COMMENT	'卖方/产权人收入类型',
  seller_employer	STRING	COMMENT	'卖方/产权人工作单位',
  seller_marital_status_name	STRING	COMMENT	'卖方/产权人婚姻状况',
  omate_name	STRING	COMMENT	'卖方/产权人配偶姓名',
  id_card_no	STRING	COMMENT	'卖方/产权人配偶身份证',
  buy_name	STRING	COMMENT	'买方姓名',
  buy_id_card_no	STRING	COMMENT	'买方证件号',
  buy_phone	STRING	COMMENT	'买方联系方式',
  buy_income_type_name	STRING	COMMENT	'买方收入类型',
  buy_employer	STRING	COMMENT	'买方工作单位',
  house_no	STRING	COMMENT	'产证编号',
  house_cert_no	STRING	COMMENT	'房产证号',
  house_area	STRING	COMMENT	'房产区域',
  house_address	STRING	COMMENT	'房产地址',
  house_type_name	STRING	COMMENT	'房产用途',
  house_acreage	STRING	COMMENT	'建筑面积',
  trading_price	DOUBLE	COMMENT	'合同成交价',
  bank_premarket_price	DOUBLE	COMMENT	'银行报备成交价',
  down_payment	DOUBLE	COMMENT	'首付款金额',
  earnest_money	DOUBLE	COMMENT	'交易定金',
  market_average_price	DOUBLE	COMMENT	'评估价',
  houseappraisal_user_name	STRING	COMMENT	'房产评估经办人',
  update_time	TIMESTAMP	COMMENT	'资质查询时间',
  audit_user_name	STRING	COMMENT	'资质查询经办人',
  archive_query_time	TIMESTAMP	COMMENT	'查档办理时间',
  archive_user_name	STRING	COMMENT	'查档经办人',
  credit_query_time	TIMESTAMP	COMMENT	'征信查询时间',
  credit_user_name	STRING	COMMENT	'征信查询经办人',
  business_query_time	TIMESTAMP	COMMENT	'工商查询时间',
  business_user_name	STRING	COMMENT	'工商查询经办人',
  litigation_query_time	TIMESTAMP	COMMENT	'法诉查询时间',
  litigation_user_name	STRING	COMMENT	'法诉查询经办人',
  pre_ransom_borrow_amount	DOUBLE	COMMENT	'预计(赎楼)贷款金额',
  guarantee_amount	DOUBLE	COMMENT	'预计保险金额',
  interview_save_time	TIMESTAMP	COMMENT	'面签保存时间',
  ori_loan_bank_name	STRING	COMMENT	'原贷银行',
  interview_time	TIMESTAMP	COMMENT	'面签时间',
  interview_user_name	STRING	COMMENT	'面签经办人',
  notarization_time	TIMESTAMP	COMMENT	'公证办理时间',
  notarization_user_name	STRING	COMMENT	'公证经办人',
  pre_ransom_date	TIMESTAMP	COMMENT	'预计赎楼时间',
  repay_method_name	STRING	COMMENT	'原贷扣款方式',
  busi_deduct_principal_balance	DOUBLE	COMMENT	'原商贷本金余额',
  fund_deduct_balance	DOUBLE	COMMENT	'公积金贷款本金余额',
  relation_loan_balance	DOUBLE	COMMENT	'原贷关联贷款余额',
  pre_fine_amount	DOUBLE	COMMENT	'预计罚息',
  prerandom_time	TIMESTAMP	COMMENT	'预约赎楼办理时间',
  prerandom_user_name	STRING	COMMENT	'预约赎楼经办人',
  is_prestore	STRING	COMMENT	'是否预存',
  prestore_day	STRING	COMMENT	'预存天数',
  new_loan_bank_name	STRING	COMMENT	'新贷银行',
  new_loan_type	STRING	COMMENT	'新贷类型',
  business_sum	DOUBLE	COMMENT	'预计商贷金额',
  biz_loan_amount_apply	DOUBLE	COMMENT	'申请商贷金额',
  new_bank_user	STRING	COMMENT	'新贷银行联系人',
  is_regulation_flag	STRING	COMMENT	'是否有监管',
  pre_first_regulation_amount	DOUBLE	COMMENT	'监管金额',
  supervision_organization	STRING	COMMENT	'资金监管机构',
  organization_account_agree_no	STRING	COMMENT	'资金监管协议编号',
  organization_applyloan_time	TIMESTAMP	COMMENT	'资金监管办理时间',
  organization_user_name	STRING	COMMENT	'资金监管经办人',
  first_regulation_bank_name	STRING	COMMENT	'首期监管款银行',
  first_regulation_amount	DOUBLE	COMMENT	'首期监管款金额',
  applyloan_time	TIMESTAMP	COMMENT	'申请贷款时间',
  applyloan_user_name	STRING	COMMENT	'申请贷款经办人',
  refund_source	STRING	COMMENT	'平台回款来源',
  fact_bu	DOUBLE	COMMENT	'新商贷金额',
  provident_fund_loan_amount	DOUBLE	COMMENT	'公积金贷款金额',
  agreeloanmark_time	TIMESTAMP	COMMENT	'同贷登记时间',
  agreeloanmark_user_name	STRING	COMMENT	'同贷登记经办人',
  ransom_bank_name	STRING	COMMENT	'赎楼贷款银行',
  ransom_borrow_amount	DOUBLE	COMMENT	'实际赎楼贷款金额',
  inputinfo_time	TIMESTAMP	COMMENT	'最终录入时间',
  inputinfo_user_name	STRING	COMMENT	'最终录入经办人',
  applycheck_time	TIMESTAMP	COMMENT	'报单时间(自动)ApplyCheck',
  applycheck_user_name	STRING	COMMENT	'报审经办人',
  approval_time	TIMESTAMP	COMMENT	'最终审查时间',
  investigate_user_name	STRING	COMMENT	'审查经办人',
  order_amount_approve	DOUBLE	COMMENT	'赎楼/借款金额',
  trustaccount_time	TIMESTAMP	COMMENT	'要件托管时间',
  is_close_flag	STRING	COMMENT	'是否关闭网银',
  trustaccount_user_name	STRING	COMMENT	'要件托管经办人',
  costitemmark_time	TIMESTAMP	COMMENT	'费项登记时间',
  costitemmark_user_name	STRING	COMMENT	'费项登记经办人',
  costconfirm_time	TIMESTAMP	COMMENT	'缴费确认时间',
  costconfirm_user_name	STRING	COMMENT	'缴费确认经办人',
  rakebackfee_fee_value	DOUBLE	COMMENT	'合计-代收费用',
  substitutefeecount_fee_value	DOUBLE	COMMENT	'渠道返佣-应收',
  user_name	STRING	COMMENT	'缴费录入经办人',
  trans_day	STRING	COMMENT	'首次缴费时间',
  pushoutcommand_time	TIMESTAMP	COMMENT	'外传指令推送时间',
  pushoutcommand_user_name	STRING	COMMENT	'外传指令推送经办人',
  sendverifymaterial_time	TIMESTAMP	COMMENT	'资料外传时间',
  sendverifymaterial_user_name	STRING	COMMENT	'资料外传经办人',
  underwrite_pass_time	TIMESTAMP	COMMENT	'核保通过时间',
  insurance_period	STRING	COMMENT	'保险期限/履约期',
  policyapply_time	TIMESTAMP	COMMENT	'出保单申请时间',
  policyapply_user_name	STRING	COMMENT	'出保单申请经办人',
  receive_policy_time	TIMESTAMP	COMMENT	'接收保单时间',
  policy_no	STRING	COMMENT	'保单号码',
  loan_amount	DOUBLE	COMMENT	'保单金额',
  premium_amount	DOUBLE	COMMENT	'保费-实缴',
  premium_payment_time	TIMESTAMP	COMMENT	'保费缴纳时间',
  policy_start	TIMESTAMP	COMMENT	'保单起期',
  policy_end	TIMESTAMP	COMMENT	'保单止期',
  insuranceinfoconfirm_time	TIMESTAMP	COMMENT	'保单确认时间',
  pushloancommand_time	TIMESTAMP	COMMENT	'放款指令推送时间',
  pushloancommand_user_name	STRING	COMMENT	'放款指令推送经办人',
  archive_query_time_2	TIMESTAMP	COMMENT	'二次查档时间',
  archive_user_name_2	STRING	COMMENT	'二次查档经办人',
  litigation_query_time_one_2	TIMESTAMP	COMMENT	'二次个诉查询时间(单篇)',
  litigation_user_name_one_2	STRING	COMMENT	'二次个诉查询经办人(单篇)',
  litigation_query_time_all_2	TIMESTAMP	COMMENT	'二次个诉查询时间(全文)',
  litigation_user_name_all_2	STRING	COMMENT	'二次个诉查询经办人(全文)',
  test_time	TIMESTAMP	COMMENT	'账户测试时间',
  test_user_name	STRING	COMMENT	'账户测试经办人',
  sendloancommand_time	TIMESTAMP	COMMENT	'发送放款指令办理时间',
  sendloancommand_user_name	STRING	COMMENT	'发送放款指令经办人',
  source_funds	STRING	COMMENT	'确认资金到账-资金来源',
  source_funds_name	STRING	COMMENT	'赎楼资金来源',
  con_funds_time	TIMESTAMP	COMMENT	'平台资金到账时间',
  loan	DOUBLE	COMMENT	'(平台)到账金额',
  loan_terms	DOUBLE	COMMENT	'产品期限',
  borrowing_value_date	TIMESTAMP	COMMENT	'借款起息日(客户借款人收到钱)',
  confirmarrival_time	TIMESTAMP	COMMENT	'确认资金到账办理时间',
  confirmarrival_user_name	STRING	COMMENT	'确认资金到账经办人',
  transferconfirm_time_apply	TIMESTAMP	COMMENT	'资金划拨申请时间',
  transferconfirm_user_name_apply	STRING	COMMENT	'资金划拨申请经办人',
  transferconfirm_time_confirm	TIMESTAMP	COMMENT	'资金划拨确认时间',
  arrival_fund	DOUBLE	COMMENT	'资金划拨确认金额',
  transferconfirm_user_name_confirm	STRING	COMMENT	'资金划拨确认经办人',
  transferconfirm_time	TIMESTAMP	COMMENT	'赎楼资金划转时间外勤岗',
  transferconfirm_ransom_cut_amount	DOUBLE	COMMENT	'赎楼扣款金额',
  ransom_flag	STRING	COMMENT	'赎楼状态',
  ransom_cut_time	TIMESTAMP	COMMENT	'赎楼扣款时间',
  ransom_fail_reason	STRING	COMMENT	'赎楼失败原因',
  pre_ransom_next_time	TIMESTAMP	COMMENT	'预计下次赎楼时间',
  obtain_cert_expect_time	TIMESTAMP	COMMENT	'预计取证时间',
  randommark_time	TIMESTAMP	COMMENT	'赎楼登记办理时间',
  randommark_user_name	STRING	COMMENT	'赎楼登记经办人',
  ransom_amount_back_flag	STRING	COMMENT	'是否划回赎楼资金',
  ransom_amout_back	DOUBLE	COMMENT	'赎楼划回金额',
  getcancelmaterial_time	TIMESTAMP	COMMENT	'取注销资料时间',
  getcancelmaterial_user_name	STRING	COMMENT	'取注销资料经办人',
  canclemortgage_time	TIMESTAMP	COMMENT	'注销抵押时间',
  canclemortgage_user_name	STRING	COMMENT	'注销抵押经办人',
  transferin_time	TIMESTAMP	COMMENT	'过户递件时间',
  transferout_time_pre	TIMESTAMP	COMMENT	'预计过户出件时间',
  transferin_user_name	STRING	COMMENT	'过户递件经办人',
  transferout_time	TIMESTAMP	COMMENT	'过户出件时间',
  transferout_user_name	STRING	COMMENT	'过户出件经办人',
  sendloanrelesecommand_time	TIMESTAMP	COMMENT	'发送款项释放指令时间',
  bank_tail_loan_amount	DOUBLE	COMMENT	'款项释放金额',
  sendloanrelesecommand_user_name	STRING	COMMENT	'发送款项释放指令经办人',
  mortgagepass_time	TIMESTAMP	COMMENT	'抵押递件时间',
  mortgageout_time_pre	TIMESTAMP	COMMENT	'预计抵押出件时间',
  mortgagepass_user_name	STRING	COMMENT	'抵押递件经办人',
  mortgageout_time	TIMESTAMP	COMMENT	'抵押出件时间',
  mortgageout_user_name	STRING	COMMENT	'抵押出件经办人',
  overinsurance_time	TIMESTAMP	COMMENT	'解保时间',
  overinsurance_user_name	STRING	COMMENT	'解保经办人',
  fact_pay_source	STRING	COMMENT	'实际回款来源',
  paymentarrival_user_name	STRING	COMMENT	'回款资金到账经办人',
  returnapply_time	TIMESTAMP	COMMENT	'资金归还申请时间',
  returnapply_user_name	STRING	COMMENT	'资金归还申请经办人',
  payment_way	STRING	COMMENT	'(平台)回款方式',
  return_funded_time	TIMESTAMP	COMMENT	'资金归还确认时间',
  fact_re_capital	DOUBLE	COMMENT	'还款金额',
  financialarchiveevent_time	TIMESTAMP	COMMENT	'财务归档时间',
  financialarchiveevent_user_name	STRING	COMMENT	'财务归档经办人',
  prefile_time	TIMESTAMP	COMMENT	'归档时间',
  prefile_user_name	STRING	COMMENT	'归档经办人',
  rev	bigint	comment	'系统版本号',
  refused_options	STRING	COMMENT	'退单原因',
  investigate_status	STRING	COMMENT	'审查结果',
  investigate_opinion	STRING	COMMENT	'审查意见',
  is_missing_materials	STRING	COMMENT	'是否补件',
  city_name	STRING	COMMENT	'分公司',
  branch_name	STRING	COMMENT	'子公司',
  borrower_type	STRING	COMMENT	'新贷借款人类型',
  down_house_survey_time	TIMESTAMP	COMMENT	'下户时间',
  down_house_survey_user	STRING	COMMENT	'下户经办人',
  approve_feedback_time	TIMESTAMP	COMMENT	'合作机构审批时间',
  is_relate_order	STRING	COMMENT	'是否关联',
  loan_time_xg	TIMESTAMP	COMMENT	'放款时间_销管',
  max_approval_time	TIMESTAMP	COMMENT	'终审时间',
  max_approval_status	STRING	COMMENT	'终审结果',
  max_payment_time	TIMESTAMP	COMMENT	'(客户)最终回款日',
  total_payment	STRING	COMMENT	'累计回款金额',
  house_num	bigint	comment	'房产数量',
  interview_num_xg	DOUBLE	COMMENT	'面签笔数_销管',
  loan_num_xg	DOUBLE	COMMENT	'放款笔数_销管',
  release_amount_xg	DOUBLE	COMMENT	'放款金额_销管',
  isjms	STRING	COMMENT	'是否加盟',
  arrival_time	TIMESTAMP	COMMENT	'确认资金到账-到账时间',
  login_account_name	STRING	COMMENT	'登入账号名',
  transfer_mark_time	TIMESTAMP	COMMENT	'放款登记时间',
  main_borrower	STRING	COMMENT	'主借款人',
  borrower_type_name	STRING	COMMENT	'原贷借款人类型',
  total_loan_balance	double	COMMENT	'原贷本金余额',
  is_relate_ransom_floor	STRING	COMMENT	'是否关联赎楼',
  in_reserve_house	STRING	COMMENT	'是否备用房',
  total_loan_amount	double	COMMENT	'新贷金额',
  order_credit_parse_status	STRING	COMMENT	'征信解析状态_订单',
  policy_name	STRING	COMMENT	'调用规则',
  order_is_employed	STRING	COMMENT	'自雇状态_订单',
  investigate_time	TIMESTAMP	COMMENT	'审查时间',
  actual_trading_price	STRING	COMMENT	'实际成交价',
  transaction_mortgage_time	TIMESTAMP	COMMENT	'办理抵押材料时间',
  transaction_mortgage_user	STRING	COMMENT	'办理抵押材料经办人',
  transaction_loan_file_time	TIMESTAMP	COMMENT	'办理放款材料时间',
  transaction_loan_file_user	STRING	COMMENT	'办理放款材料经办人',
  agreeloanresult_time	TIMESTAMP	COMMENT	'确认银行放款办理时间',
  bank_loan_time	TIMESTAMP	COMMENT	'确认银行放款时间',
  agreeloanresult_user	STRING	COMMENT	'确认银行放款经办人',
  bank_loan_amount	DOUBLE	COMMENT	'确认银行放款金额',
  transfer_mark_user	STRING	COMMENT	'放款登记经办人',
  new_house_no	STRING	COMMENT	'新房产证号',
  count_head_tail	STRING	COMMENT	'费用算头尾方式',
  input_info_complete_time	TIMESTAMP	COMMENT	'录入完成时间',
  man_check_first	STRING	COMMENT	'面签/审批顺序',
  transfer_owner_trading_price	double	COMMENT	'过户价格',
  price_source	STRING	COMMENT	'评估机构',
  has_second_mortgage	STRING	COMMENT	'原贷是否二押',
  third_type	STRING	COMMENT	'新贷第三方类型',
  new_loan_rate	double	COMMENT	'新商贷利率',
  con_funds_cost	double	COMMENT	'确认资金到账-到账金额',
  query_time_first_all	TIMESTAMP	COMMENT	'个诉查询时间(全文)',
  query_user_first_all	STRING	COMMENT	'个诉查询经办人(全文)',
  query_time_first_one	TIMESTAMP	COMMENT	'个诉查询时间(单篇)',
  query_user_first_one	STRING	COMMENT	'个诉查询经办人(单篇)',
  follow_up_username	STRING	COMMENT	'跟进人',
  rob_user_name	STRING	COMMENT	'跟单人',
  check_res_ensure_opinion	STRING	COMMENT	'合作机构审批结果',
  policy_state	STRING	COMMENT	'订单状态_银合',
  osales_user_name	STRING	COMMENT	'初始销售人员',
  osales_time	TIMESTAMP	COMMENT	'销售人员变更时间',
  nsalesusername	STRING	COMMENT	'变更后销售人员',
  sendloancommand_start_time	STRING	COMMENT	'发送放款指令时间',
  manual_end_time	TIMESTAMP	COMMENT	'订单终止时间',
  trans_time	TIMESTAMP	COMMENT	'缴费录入提交时间',
  rank	bigint	comment	'辅助列',
  auxiliary_apply_no	STRING	COMMENT	'配套订单号',
  auxiliary_product_name	STRING	COMMENT	'配套产品',
  loan_time_yy	TIMESTAMP	COMMENT	'放款时间_运营',
  loan_amount_yy	DOUBLE	COMMENT	'放款金额_运营',
  sendloan_time	TIMESTAMP	COMMENT	'通知放款时间',
  urgent_day	BIGINT	COMMENT	'加急天效',
  urgent_time	TIMESTAMP	COMMENT	'最终加急时间',
  sharp_time	TIMESTAMP	COMMENT	'最终销急时间',
  inputinfocomplete_time	TIMESTAMP	COMMENT	'最终录入完成时间',
  inputinfo_create_time	TIMESTAMP	COMMENT	'最终录入创建时间',
  investigate_create_time	TIMESTAMP	COMMENT	'最终审查创建时间',
  inputinfocomplete_time_create_time	TIMESTAMP	COMMENT	'最终录入完成创建时间',
  inputinfo_time_min	TIMESTAMP	COMMENT	'录入时间',
  inputinfo_user_name_min	String	COMMENT	'录入经办人',
  insured_amount_cardinality	String	COMMENT	'保额计费基数',
  insurance_premium_cardinality	String	COMMENT	'保费计费基数',
  charging_cardinality	String	COMMENT	'收费计费基数',
  insurer_name	string	comment	'投保人',
  insurer_cert_id	string	comment	'投保人证件号码',
  total_borrow_loan_amount	DOUBLE	COMMENT	'借款总金额',
  paymensave_user_name	String	COMMENT	'确认回款保存经办人',
  agree_loan_source_name	String	COMMENT	'同贷来源',
  market_remark	String	COMMENT	'评估反馈',
  market_create_time	TIMESTAMP	COMMENT	'评估创建日期',
  market_status_name	String	COMMENT	'评估状态',
  market_result	string	COMMENT	'正评情况',
  min_costmark_complete_time	TIMESTAMP	COMMENT	'费率登记时间',
  interview_is_outside_handle	STRING	COMMENT	'是否外出面签',
  certificate_keep_name	 STRING	COMMENT	'产证保管',
  prerandom_is_outside_handle	STRING	COMMENT	'是否外出预约赎楼',
  fact_method_name STRING	COMMENT	'实际扣款方式',
  trustaccount_is_outside_handle	 STRING 	COMMENT	'是否外出托管',
  queryarchive_is_outside_handle	STRING	COMMENT	'是否外出查档',
  canclemortgage_is_outside_handle	STRING	COMMENT	'是否外出注销抵押',
  mortgagepass_is_outside_handle	STRING	COMMENT	'是否外出抵押递件',
  transferin_is_outside_handle	STRING	COMMENT	'是否外出过户递件',
  transferout_is_outside_handle	STRING	COMMENT	'是否外出过户出件',
  test_channel_name	STRING	COMMENT	'测试方式',
  label STRING  COMMENT '标签',
  returnconfirm_user_name STRING  COMMENT '资金归还确认经办人',
  approvalresult_time STRING  COMMENT '审批结果确认办理时间',
  approvalresult_user_name STRING  COMMENT '审批结果确认经办人',
  inputinfocomplete_user_name STRING  COMMENT '录入完成经办人',
  loan_success_time STRING  COMMENT '放款结果返回-放款时间',
  is_interview_name STRING  COMMENT '是否需运营面谈',
  cooperative_guarantee_agency STRING  COMMENT '合作担保机构'
) STORED AS PARQUET;
with tmp_account as (
SELECT t.`apply_no`,
concat_ws(',',collect_set( t.`name`)) account_name
FROM ods.`ods_bpms_biz_account_common` t
WHERE t.`type` IN ('AJHKZH','AJHKK','GJJHKZH') GROUP BY apply_no
),
tmp_query_aptitude as (
select bqa.apply_no,
  bqa.update_time,  --  资质查询时间
  bqa.audit_user_name, --  资质查询经办人
  ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_query_aptitude bqa
),
tmp_docking_result as (
  SELECT apply_no,max(create_time) approve_feedback_time FROM
     ods_bpms_biz_query_docking_result where  bus_type = 'FINAL_APPROVE_RESULT'
GROUP BY apply_no
),
tmp_custormer as (
select a.apply_no,
concat_ws(',',collect_list(a.name)) omate_name, -- 卖方配偶姓名
concat_ws(',',collect_list(a.phone)) omate_phone,
concat_ws(',',collect_list(a.id_card_no)) omate_id_card_no, -- 配偶证件号码
concat_ws(',',collect_list(a.income_type_name)) omate_income_type_name,
concat_ws(',',collect_list(employer)) omate_employer,
concat_ws(',',collect_list(marital_status_tag)) omate_marital_status_name
from ods_bpms_biz_customer_rel_common a where a.relation_name like '%卖方配偶%'
group by a.apply_no
),
tmp_cost as
(
  select a.apply_no,
  a.trans_day,  --交易日
  a.trans_type, --交易类型
  a.create_time,
  a.trans_money, --交易金额
  b.fullname_ user_name,
  ROW_NUMBER() OVER(PARTITION BY apply_no,
  trans_type ORDER BY   trans_day asc) rn
from ods_bpms_c_cost_trade a left join ods_bpms_sys_user b on a.create_user_id = b.id_
),
tmp_acount_1 as
(
select
 apply_no,
 account_no,
 account_type, --资料托管类型  身份证托管：SFZ 银行卡托管：YHK  房地产证：FDCZ  其他要件托管：QT
 account_name, --平台回款卡户名
 card_type, -- 托管银行卡类型供楼卡:GLK  按揭回款卡:AJHKK 首期款回款卡:SQKHKK
 is_trust_flag, -- 是否托管
 is_close_flag,  --是否关闭网银
 ROW_NUMBER() OVER(PARTITION BY apply_no,card_type ORDER BY id desc) rn
from ods_bpms_biz_project_account
),

tmp_acount_1_close as
(
select
apply_no,
max(is_close_flag) is_close_flag --是否关闭网银
from ods_bpms_biz_project_account t
where t.card_type IN ('AJHKZH','AJHKK','GLK')
group by apply_no
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
),
-- 1:1
tmp_partner_grant as (
select apply_no,
payee,
repay_name_back,   --  划回卡户名
repay_number_back,   --  划回卡帐号
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_partner_grant
),
tmp_charge_account as (
select apply_no,
payee,   --  划回收款方(公司账号) 失败
charge_number,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_charge_account
),
tmp_cost_trade as (
SELECT
`apply_no`,
trans_type, -- 交易类型
SUM(`trans_money`) trans_money --  资金划拨申请金额
FROM ods_bpms_c_cost_trade WHERE `trans_type`='CD01' GROUP BY `apply_no`,trans_type
),
tmp_cost_trade_all as (
SELECT
t1.`apply_no`,
SUM((CASE WHEN t1.`trans_type`='CSC1' THEN t1.`trans_money` ELSE 0 END)) AS yushoufuwufei,
SUM((CASE WHEN t1.`trans_type`='CSD1' and t1.trade_status ='1'  THEN t1.`trans_money` ELSE 0 END)) AS tuifei_fee
FROM ods_bpms_c_cost_trade t1  GROUP BY `apply_no`
),
tmp_cost_trade_capital as (
SELECT
`apply_no`,
trans_type, -- 交易类型
max(trans_money) trans_money_max --  资金划拨申请金额
FROM ods_bpms_c_cost_trade WHERE `trans_type`='CC02' GROUP BY `apply_no`,trans_type
),
tmp_fd_advance_ret as (
select
apply_id,
`ret_amt`, -- 回款金额
`fin_date`, -- 计财部处理时间
ROW_NUMBER() OVER(PARTITION BY apply_id ORDER BY create_time desc) rn
from ods_bpms_fd_advance_ret
),

tmp_fd_advance as (
select
id,
apply_no,
apply_user_id, -- 垫资申请人
fin_date,  --  赎楼前垫资时间(机构暂时打不出钱)
apply_amt, --  赎楼前垫资金额
adv_type,  --垫资类型
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time asc) rn
from ods_bpms_fd_advance
),
tmp_fd_advance_fin as
(
SELECT t1.`apply_no`,
t1.adv_type,
t1.`apply_amt`,  --  赎楼前垫资金额
t1.`fin_date` fin_date, --  赎楼前垫资时间(机构暂时打不出钱)
t2.`ret_amt`, -- 回款金额
t2.`fin_date` fin_date_ret -- 计财部处理时间
FROM tmp_fd_advance t1
LEFT JOIN (select * from tmp_fd_advance_ret where rn=1) t2 ON t2.apply_id=t1.id
WHERE t1.`adv_type`='expireAdvance' AND t1.rn=1
),
tmp_test_record as (
select a.apply_no,
b.account_id,
b.test_time, --  出款前账户测试时间网银
b.handle_user_name, --  出款前账户测试人员网银
sd1.NAME_ test_channel_name,
ROW_NUMBER() OVER(PARTITION BY a.apply_no ORDER BY b.test_time desc) rn
from (SELECT
t.`apply_no`
,MAX(id) id  -- 去最后一次测试的id
FROM ods_bpms_biz_project_account  t
WHERE  t.`card_type` IN  ('AJHKZH','AJHKK','GLK')
GROUP BY apply_no)a
left join ods_bpms_biz_test_record b on b.account_id=a.id
left join (select DISTINCT KEY_,NAME_new_ NAME_ from ods_bpms_sys_dic_common where TYPE_ID_='10000001220297' or TYPE_ID_='10000010830911' or TYPE_ID_='10000057530172' or TYPE_ID_='10000034210010' or TYPE_ID_='10000025010002' or  TYPE_ID_='10000001220223' or TYPE_ID_='10000034210010' or TYPE_ID_='1000000122021' or TYPE_ID_='10000033770002' ) sd1 on sd1.KEY_= lower(b.test_channel)
),
tmp_insurance_info as (
select apply_no,
insurance_name, --保险名称
insurer_name, --投保人姓名
insurer_cert_id, -- 投保人证件号
underwrite_pass_time, --  核保完成时间
insurance_period, -- 保险期限/履约期
receive_policy_time, -- 接收保单时间
premium_payment_time, --  保费缴纳日期
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_insurance_info
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
-- 监管
tmp_supervision as
(
select apply_no,
max(amount) amount,
concat_ws(' ',collect_set(organization)) organization , -- 监管机构
concat_ws(' ',collect_set(account_agree_no)) account_agree_no --资金监管协议号
from ods_bpms_biz_supervision_common group by apply_no
),

tmp_personal_litigation as(
select apply_no,
type, -- 类型-one：单篇；all：全文
`name`,
query_time, --  个人诉讼查询时间
query_user_name, --  个人诉讼查询经办人
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY query_time asc) rn
from (
select a.apply_no,a.customer_no,a.name,b.type,b.query_time,b.query_user_name from ods_bpms_biz_customer_rel_common a
left join ods_bpms_biz_personal_litigation b on b.customer_no=a.customer_no
)T
),
tmp_personal_litigation_one as (
select * from tmp_personal_litigation where type='ONE' and rn=2
),
tmp_personal_litigation_all as (
select a.* from (select * from tmp_personal_litigation where type='ONE')  a join(
select apply_no,max(rn) rn from tmp_personal_litigation where (rn>=2 or rn>=4) and type='ONE'
group by apply_no) b on b.apply_no=a.apply_no and b.rn=a.rn
),

tmp_personal_litigation_one_first as (
   select
   apply_no
   ,query_time -- 首次个诉查询时间(单篇)
   ,query_user_name -- 首次个诉查询经办人(单篇)
   from (
      select
      apply_no, query_time, query_user_name
      ,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY query_time asc) rank
      from ods.ods_bpms_biz_personal_litigation
      WHERE TYPE ="ONE"
   ) as a
   where rank = 1
),

tmp_personal_litigation_all_first as (
   select
   apply_no
   ,query_time -- 首次个诉查询时间(全文)
   ,query_user_name -- 首次个诉查询经办人(全文)
   from (
      select
      apply_no, query_time, query_user_name
      ,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY query_time asc) rank
      from ods.ods_bpms_biz_personal_litigation
      WHERE TYPE ="ALL"
   ) as a
   where rank = 1
),

tmp_business as (
select apply_no,
query_time, -- 工商信息查询时间
query_user_name, -- 工商信息查询经办人
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY query_time desc) rn
from (
select a.apply_no,a.customer_no,b.query_time,b.query_user_name from ods_bpms_biz_customer_rel_common a
left join ods_bpms_biz_business_info b on b.customer_no=a.customer_no
where b.query_time is not null and b.query_time!=''
)T
),

tmp_credit as (
select apply_no,
operate_user_name ,--查征信经办人
parse_time manual_credit_query_date,--  征信查询时间
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY parse_time desc) rn
from (
select a.apply_no,a.customer_no,b.operate_user_name,b.parse_time from ods_bpms_biz_customer_rel_common a
left join ods_bpms_biz_query_credit b on b.customer_no=a.customer_no
where b.parse_time is not null and b.parse_time!=''
)T
),

tmp_archive_2 as (
select b.apply_no,b.auditor_name_ query_user_name,b.complete_time_ query_time from (select apply_no,max(rn) max_rn,min(rn) min_rn from (select * from ods_bpms_bpm_check_opinion_common_ex where task_key_new_='queryarchive'
and status_<>'manual_end') T group by apply_no) a
join (select * from ods_bpms_bpm_check_opinion_common_ex where task_key_new_='queryarchive'
and status_<>'manual_end') b on b.apply_no=a.apply_no and b.rn=a.max_rn
where a.max_rn>=2
),

tmp_estimate as(
  -- 线上使用
select apply_no,
nvl(house_evaluation_price,market_average_price) market_average_price, -- 银行报备成交价_万元 市场均价
remark market_remark,
create_time market_create_time,
total_price, --  评估价_元 查评估总价12
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bpms_biz_query_estimate_common where market_average_price>10000
),

tmp_seel as (
select a.apply_no,
concat_ws(',',collect_list(a.name)) seller_name, -- 卖方：SEL
concat_ws(',',collect_list(a.phone)) seller_phone, --手机
concat_ws(',',collect_list(a.id_card_no)) seller_id_card_no, -- 身份证
concat_ws(',',collect_list(employer)) employer, --工作单位
concat_ws(',',collect_list(income_type_name)) income_type_name, -- 收入类型
concat_ws(',',collect_list(marital_status_tag)) marital_status_name -- 婚姻状态
from (select * from ods_bpms_biz_customer_rel_common order by is_actual_borrower_name desc) a where  a.`role`='OWN' and (a.relation_name='产权人' or a.relation_name='产权人配偶'
or a.relation_name='新贷款借款人' or a.relation_name='卖方' or (a.is_actual_borrower_name='Y' and a.relation_name='原贷款借款人'))
group by a.apply_no
),

tmp_buy as (
select a.apply_no,
concat_ws(',',collect_list(a.name)) buy_name, -- 买方：BUY
concat_ws(',',collect_list(a.phone)) buy_phone,
concat_ws(',',collect_list(a.id_card_no)) buy_id_card_no,
concat_ws(',',collect_list(a.income_type_name)) income_type_name,  -- 收入类型
concat_ws(',',collect_list( employer)) employer, -- 工作单位
concat_ws(',',collect_list(marital_status_tag)) marital_status_name  -- 婚姻状态
from ods_bpms_biz_customer_rel_common a where  a.`role`='BUY' and a.`relation`='BUY'  group by a.apply_no
),

T as (
select bao.apply_no,
case
 when (bao.product_name like '%提放保%') then nvl(bnl.biz_loan_amount,bim.bank_loan_amount)
 when (bao.product_name like '%买付保%') then bfs.guarantee_amount
 when (bao.product_name like '%及时贷%' and bao.product_name not like '%贷款服务%') or bao.product_name = "大道房抵贷" then bfs.borrowing_amount
else
    bnl.biz_loan_amount
end order_amount --  审批金额_元(借款金额)
from ods_bpms_biz_apply_order_common bao
LEFT JOIN ods_bpms_biz_fee_summary_common bfs on bfs.apply_no=bao.apply_no
LEFT JOIN ods_bpms_biz_isr_mixed bim on bim.apply_no=bao.apply_no
LEFT JOIN (select * from ods.ods_bpms_biz_new_loan_common where rn = 1) bnl on bnl.apply_no=bao.apply_no
),
tmp_org as (
select
org_code,
org_name_, -- 所属团队
concat_ws(',',collect_set(fullname_)) org_leader -- 团队长_市场主管
from ods_bpms_sys_org_user_common  where user_post like '%市场%主管%'
and status_=1
group by org_code,org_name_
),

tmp_opinion as(
select
tmp.PROC_INST_ID_
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) UserTask3_COMPLETE_TIME_  -- 报审时间
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_STATUS_
,min(case when tmp.TASK_KEY_ = 'UserTask2' then tmp.COMPLETE_TIME_ end ) UserTask2_COMPLETE_TIME_  -- 报审时间
,min(case when tmp.TASK_KEY_ = 'ApplyCheck' then tmp.COMPLETE_TIME_ end ) ApplyCheck_COMPLETE_TIME_  -- 报审时间
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_STATUS_ -- 审查结果
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_STATUS_ -- 审查结果
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_STATUS_ -- 审查结果
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.AUDITOR_NAME_ end ) UserTask4_AUDITOR_NAME_ -- 审查经办人
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.AUDITOR_NAME_ end ) UserTask3_AUDITOR_NAME_ -- 审查经办人
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.AUDITOR_NAME_ end ) Investigate_AUDITOR_NAME_ -- 审查经办人
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.AUDITOR_NAME_ end ) ManCheck_AUDITOR_NAME_ -- 审查经办人
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.OPINION_ end ) UserTask4_OPINION_ -- 审查意见
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.OPINION_ end ) UserTask3_OPINION_ -- 审查意见
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.OPINION_ end ) ManCheck_OPINION_ -- 审查意见
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.OPINION_ end ) Investigate_OPINION_ -- 审查意见
,min(case when tmp.TASK_KEY_ = 'agreeLoanResult' then tmp.COMPLETE_TIME_ end) agreeLoanResult_time -- 确认银行放款时间
,min(case when tmp.TASK_KEY_ = 'agreeLoanResult' then tmp.AUDITOR_NAME_ end ) agreeLoanResult_user -- 确认银行放款经办人
from (
  select
    b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_) rank
    from ods_bpms_bpm_check_opinion b
    where b.COMPLETE_TIME_ is not null
) tmp
where rank = 1
group by tmp.PROC_INST_ID_
),

tmp_opinion_desc as(
select
tmp.PROC_INST_ID_
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.STATUS_ end ) UserTask4_STATUS_
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.STATUS_ end ) UserTask3_STATUS_
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.STATUS_ end ) ManCheck_STATUS_
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.STATUS_ end ) Investigate_STATUS_
,min(case when tmp.TASK_KEY_ = 'Interview' then tmp.COMPLETE_TIME_ end ) interview_time -- 面签时间
,min(case when tmp.TASK_KEY_ = 'UserTask2' and TASK_NAME_ = "面签" then tmp.COMPLETE_TIME_ end ) interview_time_v1 -- 面签时间
,min(case when tmp.TASK_KEY_ = 'DownHouseSurvey' then tmp.COMPLETE_TIME_ end ) DownHouseSurvey_time -- 下户时间
,min(case when tmp.TASK_KEY_ = 'DownHouseSurvey' then tmp.AUDITOR_NAME_ end ) DownHouseSurvey_user -- 下户经办人
,min(case when tmp.TASK_KEY_ = 'UserTask5' then tmp.COMPLETE_TIME_ end ) send_loan_command_time_v1 -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'SendLoanCommand' then tmp.COMPLETE_TIME_ end ) send_loan_command_time -- 发送放款指令时间
,min(case when tmp.TASK_KEY_ = 'AgreeLoanMark' then tmp.COMPLETE_TIME_ end ) agreeloanmark_time  --  同贷信息登记时间
,min(case when tmp.TASK_KEY_ = 'CostConfirm' then tmp.COMPLETE_TIME_ end ) costconfirm_time -- 缴费确认时间
,min(case when tmp.TASK_KEY_ = 'UserTask4' then tmp.COMPLETE_TIME_ end ) investigate_time_tfb -- 审查时间  郑州提放保  "NSL-TFB371", "SL-TFB371"
,min(case when tmp.TASK_KEY_ = 'ManCheck' then tmp.COMPLETE_TIME_ end ) investigate_time_v2 -- 审查时间
,min(case when tmp.TASK_KEY_ = 'Investigate' then tmp.COMPLETE_TIME_ end ) investigate_time_v25 -- 审查时间
,min(case when tmp.TASK_KEY_ = 'UserTask3' then tmp.COMPLETE_TIME_ end ) investigate_time_v1 -- 审查时间
,min(case when tmp.TASK_KEY_ = 'RandomMark' then tmp.COMPLETE_TIME_ end ) random_mark_time -- 赎楼登记时间
,min(case when tmp.TASK_KEY_ = 'CancleMortgage' then tmp.COMPLETE_TIME_ end )  cancle_mortage_time -- 注销抵押时间
,min(case when tmp.TASK_KEY_ = 'TransferIn' then tmp.COMPLETE_TIME_ end ) transfer_in_time  -- 过户递件时间
,min(case when tmp.TASK_KEY_ = 'TransferOut' then tmp.COMPLETE_TIME_ end ) transfer_out_time -- 过户出件
,min(case when tmp.TASK_KEY_ = 'MortgagePass' then tmp.COMPLETE_TIME_ end ) mortgage_pass_time -- 抵押递件时间
,min(case when tmp.TASK_KEY_ = 'MortgageOut' then tmp.COMPLETE_TIME_ end ) mortgage_out_time -- 抵押出件时间
,min(case when tmp.TASK_KEY_ = 'OverInsurance' then tmp.COMPLETE_TIME_ end ) over_insurance_time -- 解保时间
,min(case when tmp.TASK_KEY_ = 'financialArchiveEvent' then tmp.COMPLETE_TIME_ end ) financial_archive_event_time -- 财务归档时间
,min(case when tmp.`STATUS_`='manual_end' then tmp.OPINION_ end ) refused_options  -- 退单原因
,min(case when tmp.TASK_KEY_ = 'InputInfoComplete' then tmp.COMPLETE_TIME_ end ) InputInfoComplete_time -- 录入完成时间
,min(case when tmp.TASK_KEY_ = 'CheckMarSend' then tmp.COMPLETE_TIME_ end ) CheckMarSend_time -- 审批资料外传时间
,min(case when tmp.TASK_KEY_ = 'CheckMarSend' then tmp.AUDITOR_NAME_ end ) CheckMarSend_user -- 审批资料外传经办人
,min(case when tmp.TASK_KEY_ = 'CheckResEnsure' then tmp.OPINION_ end ) CheckResEnsure_opinion -- 审批结果确定意见
from (
  select
    b.*, row_number() over(partition by b.PROC_INST_ID_, b.TASK_KEY_ order by b.COMPLETE_TIME_ desc ) rank
    from ods_bpms_bpm_check_opinion b
) tmp

where rank = 1
group by tmp.PROC_INST_ID_
),

tmp_matter_record as (
   select
   apply_no
   ,min(case when tmp.matter_key = 'Interview' then tmp.handle_time end )  interview_time -- 面签时间
   ,min(case when tmp.matter_key = 'Interview' then tmp.handle_user_name end )  interview_user -- 面签人员
   ,min(case when tmp.matter_key = 'TransferMark' then tmp.handle_time end )  transfer_mark_time -- 放款登记时间
   ,min(case when tmp.matter_key = 'TransferMark' then tmp.handle_user_name end )  transfer_mark_user -- 放款登记经办人
   from (
       select
       b.*, row_number() over(partition by b.apply_no, b.matter_key order by b.handle_time ) rank
       from ods_bpms_biz_order_matter_record b
       where b.matter_key in ('Interview', 'TransferMark')
   ) tmp
   where tmp.rank = 1
   group by tmp.apply_no
),

tmp_matter_record_d as (
   select
   apply_no
   ,min(case when tmp.matter_name = '签订借款合同' then tmp.handle_time end )  sign_loan_contract_time -- 签订借款合同时间
   ,min(case when tmp.matter_name = '签订借款合同' then tmp.handle_user_name end )  sign_loan_contract_user -- 签订借款合同经办人
   ,min(case when tmp.matter_name = '整理银行档案' then tmp.handle_time end )  sort_bank_archives_time -- 整理银行档案时间
   ,min(case when tmp.matter_name = '整理银行档案' then tmp.handle_user_name end )  sort_bank_archives_user -- 整理银行档案经办人
   ,min(case when tmp.matter_name = '办理抵押材料' then tmp.handle_time end )  transaction_mortgage_time -- 办理抵押材料时间
   ,min(case when tmp.matter_name = '办理抵押材料' then tmp.handle_user_name end )  transaction_mortgage_user -- 办理抵押材料经办人
   ,min(case when tmp.matter_name = '办理放款材料' then tmp.handle_time end )  transaction_loan_file_time -- 办理放款材料时间
   ,min(case when tmp.matter_name = '办理放款材料' then tmp.handle_user_name end )  transaction_loan_file_user -- 办理放款材料经办人
   from (
       select
       b.*, row_number() over(partition by b.apply_no, b.matter_key order by b.handle_time desc) rank
       from ods_bpms_biz_order_matter_record b
       where b.matter_name in ("签订借款合同", "整理银行档案", "办理抵押材料", "办理放款材料")
   ) tmp
   where tmp.rank = 1
   group by tmp.apply_no
),

tmp_order_flow as(
   select
   t1.apply_no
   ,min((CASE WHEN t3.apply_status<>'recover' and t3.apply_status <> "refuse" and t1.flow_type='mq_type' THEN t1.handle_time ELSE NULL END  )) AS mq_type_time  -- 面签时间                          ,
   ,min((CASE WHEN t3.apply_status<>'recover' and t3.apply_status <> "refuse" and t1.flow_type='mq_type' THEN t1.handle_user_name ELSE NULL END  )) AS mq_type_user  -- 面签用户
   from (
       select
       b.*, row_number() over(partition by b.apply_no, b.flow_type order by b.handle_time  ) rank
       from ods_bpms_biz_order_flow b
       where b.flow_type='mq_type'
   ) t1
   left join ods.ods_bpms_biz_apply_order_common t3 on (t1.apply_no=t3.apply_no)
   where t1.rank = 1
   group by t1.apply_no
),

tmp_missing_materials as (
   SELECT
   bmm.apply_no
   ,max(bmm.id) id
   FROM ods.ods_bpms_biz_missing_materials bmm
   where bmm.node_id = "Investigate"
   group by bmm.apply_no
),

tmp_qdrfar as (
   SELECT apply_no, max(create_time) approve_feedback_time
   FROM ods.ods_bpms_biz_query_docking_result
   WHERE bus_type = 'FINAL_APPROVE_RESULT'
   group by apply_no
),

tmp_qdrwar as (
   SELECT apply_no, max(create_time) loan_feedback_time
   FROM ods.ods_bpms_biz_query_docking_result
   WHERE bus_type = 'WZ_ACTIVATE_LIMIT_RESULT'
   group by apply_no
),
tmp_main_borrower as (
   SELECT
   a.apply_no
   ,concat_ws(',',collect_set(b.name)) main_borrower -- 主借款人
   FROM ods.ods_bpms_biz_customer_rel_common a
   left join ods.ods_bpms_biz_customer b on a.customer_no = b.cust_no
   where a.is_actual_borrower_name = "是"
   group by a.apply_no
),
tmp_relate_ransom_floor as (
   select
   bao.apply_no
   ,"是" is_relate_ransom_floor  -- 是否关联赎楼
   from ods.ods_bpms_biz_apply_order_common bao
   left join ods.ods_bpms_biz_apply_order_common b on bao.group_apply_no = b.group_apply_no
   where bao.group_apply_no is not null and bao.group_apply_no <> ""
   and b.product_name like "%赎楼%" and b.product_name not like "%无赎楼%"
   and bao.apply_no <> b.apply_no
   group by bao.apply_no
),
tmp_reserve_house as (
select
apply_no
,"是" in_reserve_house -- 是否备用房
from ods.ods_bpms_biz_house_common
where in_reserve_house = "Y"
group by apply_no
),

tmp_order_credit_parse_status as (
   -- 订单粒度
   select
   br.apply_no
   ,if(sum(case when a.parse_way <> "Y" then 1 end) >= 1, "解析失败", "解析成功") order_credit_parse_status -- 征信解析状态
   from ods.ods_bpms_biz_customer_rel br
   left join ods.ods_bpms_biz_query_credit  a on br.customer_no = a.customer_no
   group by br.apply_no
),

tmp_order_is_employed as (
   -- 订单粒度
   select
   br.apply_no
   ,if(sum(case when a.is_self_employed = "Y" then 1 end) >= 1, "自雇", "受薪") order_is_employed -- 自雇状态
   from ods.ods_bpms_biz_customer_rel br
   left join ods.ods_bpms_biz_business_info a on br.customer_no = a.customer_no
   group by br.apply_no
),

tmp_strategy as (
   SELECT
   a.apply_no
   ,a.policy_name -- 策略名
   from(
      select
       b.*, row_number() over(partition by b.apply_no order by b.create_time desc ) rank
      from ods.ods_bpms_biz_auto_approve_record b
      where b.flow_name = "录入"
   ) as a
   where a.rank = 1
),

tmp_biz_house_transfer_d as (
   select
   house_no
   ,transfer_receipt_no -- 过户回执号
   ,new_house_no -- 新房产证号
   ,mortgage_receipt_no -- 抵押回执号
   from (
      select
          b.*, row_number() over(partition by b.house_no order by b.create_time desc) rank
          from ods.ods_bpms_biz_house_transfer b
   ) as a
   where a.rank = 1
),

tmp_query_estimate_h_d as (
   select
   house_no
   ,case when price_source = "其他" then concat(price_source, '-', remark)
         else price_source
    end price_source -- 评估机构
   from(
      select
      a.house_no
      ,if(b.name_ is null or b.name_ = "", a.price_source, b.name_) price_source
      ,a.remark
      ,ROW_NUMBER() OVER(PARTITION BY a.house_no ORDER BY a.create_time desc) rank
      from ods.ods_bpms_biz_query_estimate a
      left join ods.ods_bpms_sys_dic b on a.price_source = b.key_
   ) as a
   where a.rank = 1
),

tmp_query_estimate_aj as (
   -- 大道按揭 评估机构、评估价格取值
   select
   house_no
   ,house_evaluation_company -- 评估机构
   ,house_evaluation_price -- 评估价格
   from(
      select
      a.house_no
      ,a.house_evaluation_company -- 评估机构
      ,a.house_evaluation_price -- 评估价格
      ,ROW_NUMBER() OVER(PARTITION BY a.house_no ORDER BY a.create_time desc) rank
      from ods.ods_bpms_biz_query_estimate a
      where a.house_evaluation_price is not null and a.house_evaluation_company is not null and a.house_evaluation_company <> ""
   ) as a
   where a.rank = 1
),
tmp_fd_advance_floorAdvance as (
  SELECT
  t1.`apply_no`,
  sum(t1.`adv_amt`)  AS  floorAdvance_adv_amt, -- 赎楼前垫资金额
  min(t1.`fin_date`)  AS  floorAdvance_fin_date, -- 赎楼前垫资放款时间
  max(t2.`fin_date`) AS  floorAdvance_ret_date -- 赎楼前垫资归还时间
  FROM `ods_bpms_fd_advance` t1
  LEFT JOIN `ods_bpms_fd_advance_ret` t2 ON t1.id=t2.`apply_id`
  WHERE t1.`adv_type`='floorAdvance' and t1.status <> "ZZ"
  group by t1.`apply_no`
),
tmp_warn_hist as (
select fwh.apply_no,
  fwh.warn_status,
  ROW_NUMBER() OVER(PARTITION BY apply_no,warn_level ORDER BY create_time desc) rn
  from (select * from ods_bpms_fwn_warn_history ) fwh
),
tmp_sales as (
select
apply_no,
osalesusername osalesusername_min,
nsalesusername,
datau_time,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY datau_time asc) rn
from (
select apply_no,osalesusername,nsalesusername,datau_time from ods_bpms_biz_data_update_common where apply_no is not null and
instr(lower(update_module),'salesuserinfo')>0
)T
),
tmp_sales_max as (
select a.apply_no,nsalesusername,datau_time from tmp_sales a join
(select apply_no,max(rn) max_rn from tmp_sales group by apply_no) b on b.max_rn=a.rn and b.apply_no=a.apply_no
),
tmp_all_customer as (
  select
  ts.apply_no
  ,bao.product_name
  ,bp.sfjy
  ,ts.seller_id_card_no
  ,tb.buy_id_card_no
  ,bao.apply_status_name
  from tmp_seel ts
    join tmp_buy tb on ts.apply_no = tb.apply_no
    join ods.ods_bpms_biz_apply_order_common bao on ts.apply_no = bao.apply_no
    join ods.ods_bpms_b_product bp on bao.product_name = bp.product_name
),
tmp_pt_product as (
  select
  bao.apply_no
  ,concat_ws(',',collect_list(tac_s.apply_no)) auxiliary_apply_no  -- 配套订单号
  ,concat_ws(',',collect_list(tac_s.product_name)) auxiliary_product_name -- 配套产品
  from ods.ods_bpms_biz_apply_order_common bao
  join tmp_all_customer tac on bao.apply_no = tac.apply_no
  join tmp_all_customer tac_s
    on tac.seller_id_card_no = tac_s.seller_id_card_no
    and tac.buy_id_card_no = tac_s.buy_id_card_no
    and tac_s.sfjy = 1  -- 交易类
    and tac_s.product_name <> "大道按揭"
    and tac_s.apply_status_name not in ("已终止", "拒绝")
  where tac.apply_no <> tac_s.apply_no
  group by bao.apply_no
),
tmp_checkcreate as (
select
apply_no
,MAX((CASE WHEN lower(task_key_new_)='inputinfo' THEN create_time_ ELSE NULL END))  inputinfo_create_time
,MAX((CASE WHEN lower(task_key_new_)='investigate' THEN create_time_ ELSE NULL END))  investigate_create_time
,MAX((CASE WHEN lower(task_key_new_)='inputinfocomplete' THEN create_time_ ELSE NULL END))  inputinfocomplete_create_time
,MIN((CASE WHEN lower(task_key_new_)='costmark' THEN complete_time_ ELSE NULL END))  min_costmark_complete_time
from (
select
apply_no,create_time_,task_key_new_,task_name_new_,complete_time_ from ods_bpms_bpm_check_opinion_common_ex where rn=1
and task_key_new_ in ('inputinfo','investigate','inputinfocomplete','costmark')
)T group by apply_no
),
temp_manual_end as(
SELECT PROC_INST_ID_,complete_time_,
ROW_NUMBER() OVER(PARTITION BY PROC_INST_ID_ ORDER BY complete_time_ desc) rank
from ods_bpms_bpm_check_opinion
where status_ in ('manual_end','sys_auto_end')
),
temp_biz_house as(
select
apply_no,
concat_ws(',',collect_set(house_no)) house_no,
concat_ws(',',collect_set(house_cert_no)) house_cert_no,
concat_ws(',',collect_set(house_area)) house_area,
concat_ws(',',collect_set(house_address)) house_address,
concat_ws(',',collect_set(house_type_name)) house_type_name,
concat_ws(',',collect_set(house_acreage)) house_acreage
from ods.ods_bpms_biz_house_common
where apply_no is not null
group by apply_no
),
tmp_estimate_record_history as (
select
   bsr.apply_no
  ,bsr.create_time
  ,case  bsr.house_evaluation_status
  when '0' then '未分配'
  when '1' then '已分未出'
  when '2' then '已分已出'
  when '3' then '已查收预评'
  when '4' then '已查收正评'
  when '5' then '归档'
  else null end house_evaluation_status
,null market_result
from ods_bpms_biz_estimate_record bsr where apply_no is not null
),
tmp_p2p_extend_lst as (
select apply_no,
VALUE, -- 放款结果返回-放款时间
       ROW_NUMBER() OVER ( PARTITION BY apply_no, `key` ORDER BY VALUE ) rn
from ods_bpms_biz_p2p_ret_extend
WHERE `key` = 'loanSuccessTime'
)
  insert overwrite table dwd.`dwdtmp_orders_ex`
  select
   bao.apply_no `业务编号`,
   bao.product_version `版本`,
   bao.product_name `产品名称`,
   bao.product_type `产品类型`,
   bao.transaction_type `交易类型`,
   tmp2.order_amount `金额`,
   bao.apply_status_name,--`订单状态`,
   case when bao.relate_type not in ('MAIN', 'main') then bao.group_apply_no end group_apply_no , -- 关联id
   bao.relate_type_new `关联生成关系`,
   bao.relate_type_name `关联类型`,
   case when  bim.risk_level = 'fbj' then '非标件'
        when  bim.risk_level  = 'bzyw' then '标准业务'
   else ''
   end risk_grade, -- 风险等级
   nvl(sd_pt.NAME_, bfs.price_tag)  price_tag,  -- 价格标签
   bim.tail_release_node_name `放款节点` ,
   bao.apply_time `业务申请时间`,
   bao.update_time order_update_time, -- `订单更新时间`
   obso.name_  `所属团队`,
   sys.org_leader `团队长_市场主管`,
   ban.channel_name `渠道名称`,
   ban.channel_type_name `渠道类型名称`,
   ban.channel_tag_new `渠道标签`,
   bao.sales_user_name `渠道经理`,
   ban.channel_phone,   -- `渠道电话`,
   ban.rebate_man `channel_rebate_man`,  -- `返佣人`,
   ban.rebate_bank `channel_rebate_bank`,  -- `返佣开户行`,
   ban.rebate_card_no `channel_rebate_card_no`,  -- `返佣账号`
   bao.partner_insurance_name `合作机构/保险/签约机构`,
   bao.partner_bank_name , -- `合作银行`
   bfs.own_fund_amount `卖方自有资金`,
   case when bfs.random_pay_mode = "bankPay" then "银行直接支付"
        when bfs.random_pay_mode = "companyHelpPay" then "我司协助支付"
   end random_pay_mode, -- 赎楼贷款支付方式
   bfs.tail_pay_mode `尾款支付方式`,
   t666.transfertail_time,  --  尾款划拨申请时间
   t666.transfertail_user_name, --  尾款划拨申请经办人 外勤岗
   null , --  尾款划拨申请金额_元
   t666.confirmtransfertail_time,  --  尾款划拨确认时间
   t666.confirmtransfertail_user_name, --  尾款划拨确认用户名 外勤岗
   null, -- 尾款划拨确认金额
   t666.confirmtailarrial_time, -- '确认资金到账（尾款）时间',
   bfs.fixed_term `预估用款期限`,
   tseel.seller_name `seller_name` ,  --  卖方/借款人姓名
   tseel.seller_id_card_no `seller_id_card_no`,  --  卖方/借款人证件号码
   tseel.seller_phone `seller_phone`,      --  卖方/借款人联系方式
   tseel.income_type_name `income_type_name`, --  卖方/收入类型
   tseel.employer `employer`, --工作单位
   tseel.marital_status_name,-- 婚姻状况
   bcr.omate_name `omate_name`, -- 配偶姓名
   bcr.omate_id_card_no, -- 配偶证件号码
   tbuy.buy_name,  --  买方姓名
   tbuy.buy_id_card_no,  --  买方证件号码
   tbuy.buy_phone,  --  买方联系方式
   tbuy.income_type_name buy_income_type_name,  --  买方/收入类型
   tbuy.employer buy_employer,  --  买方/收入类型
   nvl(bh.house_no,bhg.house_no), -- 房产号
   nvl(bh.house_cert_no,bhg.house_cert_no), --房产证号
   nvl(bh.house_area,bhg.house_area),--  所在区域
   nvl(bh.house_address,bhg.house_address), --  房产地址
   nvl(bh.house_type_name,bhg.house_type_name), --  房产用途
   nvl(bh.house_acreage,bhg.house_acreage), --  面积
    case when instr(bao.product_type,'按揭服务')>0 then bdi.actual_trading_price
      else bdi.trading_price end trading_price, --  成交价_元
   boe.loan_offer, -- 银行报备成交价_万元 市场均价
   bdi.down_payment, --buy首付款金额-元
   bdi.earnest_money, --  buy交易定金-元
   case when bao.product_name = "大道按揭" then tqeaj.house_evaluation_price
        else tst.market_average_price
   end market_average_price, --  评估价_元
   t666.houseappraisal_user_name, --  评估经办人 (事项)
   bqa.update_time,  --  资质查询时间
   bqa.audit_user_name, --  资质查询经办人
   t666.queryarchive_time_min, --  查档时间
   t666.queryarchive_user_name_min, -- 查档经办人事项
   tct1.manual_credit_query_date, --  征信查询时间
   tct1.operate_user_name, --查征信经办人
   tbn1.query_time, -- 工商信息查询时间
   tbn1.query_user_name gs_query_user_name, -- 工商信息查询经办人
   tbp.query_time person_query_time, --  个人诉讼查询时间
   tbp.query_user_name p_query_user_name, --  个人诉讼查询经办人
   bfs.pre_ransom_borrow_amount, --  预计（赎楼）贷款金额
   bfs.guarantee_amount, --  预计保险金额_元
   bim.interview_save_time, --  面签保存时间
   bol.ori_loan_bank_name, --  原贷款银行
   t666.interview_time_min,
   t666.interview_user_name_min, --  面签人员
   t666.notarization_time,  --  公证时间
   t666.notarization_user_name,-- 公证人员
   bol.pre_ransom_date,  --  预计赎楼时间
   bol.repay_method_name,  --  预计赎楼扣款方式
   bol.busi_deduct_principal_balance,  --  商贷本金余额_元
   bol.fund_deduct_balance,  --  公积金贷款本金余额_
   bol.relation_loan_balance, --  关联贷款余额_元
   bol.pre_fine_amount,  --  提前预计罚息_元
   t666.prerandom_time,  --  预约还款办理时间
   t666.prerandom_user_name,--   预约还款经办人
   bol.is_prestore,--  是否预存(赎楼账号是否有钱)
   bol.prestore_day,  --  预存天数
   bnl.new_loan_bank_name,  --  新贷款银行
   case when bao.product_type='按揭服务' then
   regexp_replace(regexp_replace(regexp_replace(bnl.new_loan_type,'1','商业贷款'),'2','组合贷款'),'3','纯公积金贷款')
   when instr(bao.product_name,'房抵贷')>0 then bpai.loan_usage_name
   else NULL end new_loan_type , --  贷款类型(公积金or商贷)
   bnl.business_sum,  --  预计商业贷款金额_元
   bnl.biz_loan_amount,--  申请商贷金额_万元
   bnl.new_bank_user, --  银行客户经理
   case when (tsp.account_agree_no is not null and tsp.account_agree_no<>'') then '是' else "否" end is_regulation_flag,  -- 钱是否有监管
   bnl.first_regulation_amount,  --  监管金额_元
   tsp.organization, --  资金监管机构
   tsp.account_agree_no,  --  资金监管协议编号
   case when  (bnl.first_regulation_amount>0 or tsp.amount >0) then t666.applyloan_time else NULL end  zj_applyloan_time, --  资金监管经办时间
   case when  (bnl.first_regulation_amount>0 or tsp.amount >0) then t666.applyloan_user_name else NULL end zj_applyloan_user_name , --  资金监管经办人
   bnl.`first_regulation_bank_name`,-- '首期监管款银行',
   bnl.`first_regulation_amount` f_first_regulation_amount, -- '首期监管款金额（元）',
   t666.applyLoan_time, --  按揭申请时间(新贷款) app ApplyLoan
   t666.applyLoan_user_name,  --  按揭申请经办人(新贷款) ApplyLoan
   regexp_replace(regexp_replace(regexp_replace(regexp_replace(bfs.refund_source,'^xdkzj','  新贷款资金'),'^yxjgzxdk','银行监管资金+新贷款资金'),'^yxjgzj','银行监管资金'),'^xdkzj','新贷款资金'),  --  平台回款来源
   bnl.biz_loan_amount sj_biz_loan_amount,  --  实际商业贷款金额
   bnl.provident_fund_loan_amount sj_provident_fund_loan_amount,  --  实际公积金贷款金额
   t666.agreeloanmark_time, --  核实同贷时间 (外勤)
   t666.agreeloanmark_user_name,  --  核实同贷经办人(外勤)
   bnl.new_loan_bank_name sl_new_loan_bank_name,--  赎楼贷款银行
   bfs.ransom_borrow_amount sjsl_ransom_borrow_amount ,--  实际赎楼贷款金额
   t666.inputinfo_time,  --  录入时间 InputInfo
   t666.inputinfo_user_name,    --  录入人员 InputInfo
   t666.applycheck_time, --  报单时间 (自动) ApplyCheck
   t666.applycheck_user_name,  --  报单人员(自动) ApplyCheck
   t666.investigate_time approval_time,  --  审批时间(风控)
   t666.investigate_user_name,  --  审批人员(风控)
   case when bao.product_type='现金类产品' then cast(bfs.borrowing_amount as string)
   when bao.product_type='保险类产品' then nvl(cast (bfs.ransom_borrow_amount as string),cast (brf.ransom_borrow_amount as string))
   else NULL end order_amount_approve,  --  审批金额_元(借款金额)
   t666.trustaccount_time, --  账户托管时间(证件) TrustAccount
   regexp_replace(regexp_replace(ta1_g1.is_close_flag,'Y','是'),'N','否'),
   t666.trustaccount_user_name,  --  账户托管经办人(外勤)
   t666.costitemmark_time, --  费项登记时间(渠道经理)
   t666.costitemmark_user_name, --  费项登记人(渠道经理)
   t666.costconfirm_time_min,  --  缴费确认时间(资金岗)
   t666.costconfirm_user_name_min,  --  缴费确认人(资金岗)
   dwf.substitutefeecount_fee_value,--  代收费用合计(渠道增值)
   dwf.rakebackfee_fee_value, -- 返佣费
   tc1.user_name, --  缴费录入经办人(资金岗)
   tc1.trans_day, --  缴费录入时间(资金岗)
   t666.pushoutcommand_time,  --  外传指令推送经办时间(分公司申请发给总部)
   t666.pushoutcommand_user_name,   --  外传指令推送经办人(分公司申请发给总部)
   t666.sendverifymaterial_time, --  核保资料外传时间(总公司确认发给机构)
   t666.sendverifymaterial_user_name,--  核保资料外传经办人
   tii1.underwrite_pass_time, --  核保完成时间
   tii1.insurance_period, --  保险期限/履约期
   t666.policyapply_time, --  出保单申请经办时间
   t666.policyapply_user_name,--  出保单申请经办人
   tii1.receive_policy_time,--  null jsbd_date --  接收保单时间
   tip1.policy_no, --  保单号码
   nvl(tip1.insurance_amount,tip1.loan_amount),  --  保险金额
   tip1.premium_amount,  --  保费金额
   tii1.premium_payment_time, --  保费缴纳日期
   tip1.policy_start,  --  保险起期
   tip1.policy_end, --  保险止期
   t666.insuranceinfoconfirm_time, --  保单确认经办人 InsuranceInfoConfirm
   t666.pushloancommand_time, --  放款指令推送时间(分公司申请发给总部) PushLoanCommand
   t666.pushloancommand_user_name,   --  放款指令推送经办人(分公司申请发给总部)
   tah2.query_time 2_query_time,      --  放款前二次查档时间
   tah2.query_user_name 2_query_user_name , --  放款前二次查档经办人
   tpl2.query_time 2_p_query_time, --  放款前查询时间_二次查个人诉讼查询（单篇）
   tpl2.query_user_name 2_p_query_user_name ,--  放款前个人诉讼查询经办人_二次查个人诉讼查询（单篇）
   tpla2.query_time fk_2_query_time,--  放款前查询时间_二次查个人诉讼查询（全文）
   tpla2.query_user_name, --  放款前个人诉讼查询经办人_二次查个人诉讼查询（全文）
   te.test_time,--  出款前账户测试时间网银
   te.handle_user_name, --  出款前账户测试人员网银
   t666.sendLoancommand_time sendLoancommand_time_bl,  --  发送放款指令时间(总公司确认发给机构) SendLoanCommand
   t666.sendLoancommand_user_name ,  --  发送放款指令经办人(总公司确认发给机构)
   case when cfm.source_funds='advance' then '垫资款'
      when cfm.source_funds='plat' then '平台款'
   else NULL end source_funds, --  赎楼资金来源
   regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(cfm.source_funds),'advance','垫资款'),'plat','平台款'),'bankpay','银行直接支付'),'companyhelppay','我司协助支付') source_funds_name, --  赎楼资金来源名
   CASE WHEN bao.product_type='现金类产品' THEN nvl(cast(bfs.platform_value_date as string),cast(cfm.con_funds_time as string))
         WHEN bao.product_type='保险类产品' THEN cast(bim.bank_loan_time as string)
         WHEN bao.product_type='按揭服务' THEN ''-- 平台资金到账时间(机构给总部或借款人)
   ELSE NULL END con_funds_time,
   nvl(cfm.plat_arriva_money,cfm.con_funds_cost), --  平台到账金额_元a
   bfs.product_term, -- 产品期限
   bfs.borrowing_value_date, --  借款起息日（客户借款人收到钱)
   t666.confirmarrival_time,  --  资金到账时间(确认资金到账时间)
   t666.confirmarrival_user_name,  --  资金到账经办人(确认资金到账)
   t666.transferapply_time,  --  资金划拨申请时间(机构如到公司需要划拨)资金岗
   t666.transferapply_user_name, --  资金划拨申请经办人
   t666.transferconfirm_time,  --  资金划拨确认时间 账务岗
   case
   when bao.product_type='现金类产品' then  nvl(case when cfm.source_funds='plat' then cast (cfm.con_funds_cost as string) else NULL end,cast (bim.bank_loan_amount as string))
   when bao.product_type='保险类产品' then cast (tct.trans_money as string)
   else NULL END ransom_borrow_amount , --  资金划拨确认金额
   t666.transferconfirm_user_name,  --  资金划拨确认经办人
   t666.sendLoancommand_time sendLoancommand_time_sl, --  赎楼资金划转时间 外勤岗
   case when instr(bao.product_name,'提放保')>0 then brf.ransom_cut_amount
   when instr(bao.product_name,'交易保')>0 then(case when t666.sendLoancommand_time is not null then nvl(bfs.ransom_borrow_amount,brf.ransom_borrow_amount) else NULL end)
   else NULL
   end ransom_cut_amount, --  赎楼资金划转金额_元
   case when bao.product_name='大道按揭' then ''
   else
   regexp_replace(regexp_replace(brf.ransom_flag,'Y','成功'),'N','失败') end ransom_flag,  --  赎楼状态 成没成功
   case when bao.product_type='现金类产品' then brf.ransom_cut_time else t666.randommark_time end ransom_cut_time,  --  赎楼扣款时间
   brf.ransom_fail_reason,  --  赎楼失败原因
   brf.pre_ransom_next_time,  --  预计下次赎楼时间
   bht.obtain_cert_expect_time,   --  预计取证时间 赎楼完什么给房产证
   t666.randommark_time,  --  赎楼登记时间
   t666.randommark_user_name,  --  赎楼登记经办人
   regexp_replace(brf.ransom_amount_back_flag,'Y','划回'),   --  是否划回赎楼资金 失败
   case when brf.`ransom_amount_back_flag` is not null and brf.`ransom_amount_back_flag`<>'' then nvl(bfs.ransom_borrow_amount,brf.ransom_borrow_amount)
   else null end ransom_amout_back, --  划回金额 失败
   t666.getcancelmaterial_time,  -- 领取注销资料时间  外勤a
   t666.getcancelmaterial_user_name,  --  领取注销资料经办人 外勤
   t666.canclemortgage_time,  --  注销抵押办理时间 外勤
   t666.canclemortgage_user_name, --  注销抵押经办人 外勤
   t666.transferin_time ,  --  过户递件时间 外勤
   bhtc.transfer_expect_time,  --  预计过户出件时间 外勤
   t666.transferin_user_name, --  过户递件经办人 外勤
   t666.transferout_time,  --  过户出件时间 外勤
   t666.transferout_user_name,  --  过户出件经办人 外勤
   t666.sendloanrelesecommand_time, --  发送款项释放指令时间 外勤
   bim.bank_tail_loan_amount, --  款项释放金额-元
   t666.sendloanrelesecommand_user_name,--  发送款项释放指令经办人
   t666.mortgagepass_time,--  抵押递件时间
   t666.mortgageout_time , --  预计抵押出件时间
   t666.mortgagepass_user_name,   --  抵押递件办理人员
   t666.mortgageout_time dy_mortgageout_time,   --  抵押出件时间
   t666.mortgageout_user_name,   --  抵押出件经办人
   t666.overInsurance_time, -- 解保时间
   t666.overInsurance_user_name, --  解保经办人
   regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(cfm.fact_pay_source),'^dzk$','垫资款'),'^dzkbf_other$','垫资款(部分) + 其它'),'^zck$','自筹款'),'^xdkzj$','新贷款资金'),'^ly$','离异'),'^yxjgzj$','银行监管资金'),'^yxjgzxdk$','银行监管资金+新贷款资金'),   --  实际回款来源
   t666.paymentarrival_user_name,  --  回款资金到账经办人
   t666.returnapply_time,  --  资金归还申请时间
   t666.returnapply_user_name,  --  资金归还申请经办人
   cfm.re_arrival_settl_way_name, --  回款方式
   t666.returnconfirm_time,   --  归还平台借款时间
   tctc.trans_money_max,
   t666.financialarchiveevent_time, --  财务归档时间
   t666.financialarchiveevent_user_name, --  财务归档经办人
   t666.prefile_time,  --  归档时间
   t666.prefile_user_name,   --  归档经办人
   bao.rev,--  系统版本号
   opid.refused_options tuidan_reason--  退单原因
   ,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opi.UserTask4_STATUS_   -- 郑州提放保
          when bao.product_version in ('v1.5', 'v1.0') then opi.UserTask3_STATUS_
          when bao.product_version = 'v2.0' then opi.ManCheck_STATUS_
          when bao.product_version = 'v2.5' then opi.Investigate_STATUS_
     end ,'reject','驳回'),'agree','同意'),'manual_end','人工终止'),'sys_auto_end','人工终止'),'oppose','驳回') investigate_status -- 审查结果
   , case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opi.UserTask4_OPINION_   -- 郑州提放保
          when bao.product_version in ('v1.5', 'v1.0') then opi.UserTask3_OPINION_
          when bao.product_version = 'v2.0' then opi.ManCheck_OPINION_
          when bao.product_version = 'v2.5' then opi.Investigate_OPINION_
     end investigate_user -- 审查意见
   ,case when tmm.id is not null then '是' else "否" end is_missing_materials  -- 是否补件
   ,dca.company_name_2_level city_name-- 分公司
   ,dca.company_name_3_level branch_name -- 附属公司
   ,regexp_replace(regexp_replace(lower(bnl.borrower_type),'^public$','公司'),'^personal','个人') -- 新贷款
   ,opid.DownHouseSurvey_time -- 下户时间
   ,opid.DownHouseSurvey_user -- 下户经办人
   ,qdrfar.approve_feedback_time -- 银行反馈审批结果时间
   ,if(bao.group_apply_no is not null and bao.group_apply_no <> "", "是", "否") is_relate_order  -- 是否关联订单
   ,oofc.loan_time_xg -- 放款时间_销管
   ,nvl(
     case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then cast(opid.investigate_time_tfb as string)   -- 郑州提放保
            when bao.product_version in ('v1.5', 'v1.0') then cast (opid.investigate_time_tfb as string)
            when bao.product_version = 'v2.0' then cast(opid.investigate_time_v2 as string)
            when bao.product_version = 'v2.5' then nvl(cast(opid.investigate_time_v2 as string), cast (opid.investigate_time_v25 as string))
     end -- 审批时间
     ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then cast(opid.investigate_time_tfb as string)   -- 郑州提放保
            when bao.product_version in ('v1.5', 'v1.0') then cast(opid.investigate_time_v1 as string)
            when bao.product_version = 'v2.0' then cast(opid.investigate_time_v2 as string)
            when bao.product_version = 'v2.5' then cast(opid.investigate_time_v25 as string)
      end  -- 审查时间
   ) max_approval_time -- 最终审批时间
   ,
   case when
      nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_STATUS_, opid.Investigate_STATUS_)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then opid.Investigate_STATUS_
         end  -- 审查结果
      ) = "agree" then  "同意"
    when  nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_STATUS_, opid.Investigate_STATUS_)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then opid.Investigate_STATUS_
         end -- 审查结果
      ) = "manual_end" then "人工终止"
    when nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_STATUS_, opid.Investigate_STATUS_)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then opid.Investigate_STATUS_
         end  -- 审查结果
      ) = "awaiting_check" then "待审批"

    when nvl(case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask4_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then nvl(opid.ManCheck_STATUS_, opid.Investigate_STATUS_)
          end  -- 审批结果
        ,case when bao.product_id in ("NSL-TFB371", "SL-TFB371")  then opid.UserTask4_STATUS_   -- 郑州提放保
             when bao.product_version in ('v1.5', 'v1.0') then opid.UserTask3_STATUS_
             when bao.product_version = 'v2.0' then opid.ManCheck_STATUS_
             when bao.product_version = 'v2.5' then opid.Investigate_STATUS_
         end  -- 审查结果
      ) = "reject" then "驳回"
   end max_approval_status -- 最终审批结果
   ,trade_cc01.max_cc01_time max_payment_time  -- 最后一次回款时间
   ,trade_cc01.total_cc01_money total_payment -- 累计回款金额
   ,nvl(obbh.fcsl, 0) house_num --  房产数量
   ,(case when t666.interview_time_min is null  then 0
        else
         (case
            when bao.product_name='保险服务'
               or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
               or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
               then 0
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1 and datediff(interview_time_min, "2020-04-01") >= 0 then 1
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
            when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and nvl(obbh.fcsl, 0)=0)
               then 1  -- 和放款笔数折算不同
            when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
               then 0.5
            else 0
         end )
   end ) interview_num_xg  -- 面签笔数 销管口径
   ,(case when oofc.loan_time_xg is null then 0
    else
      (case
            when bao.product_name='保险服务'
               or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%'))
               or (bao.product_type='现金类产品' and (bao.relate_type_name in('标的拆分','到期更换平台') or nvl(sd_pt.NAME_, bfs.price_tag) in ('保险垫资','限时贷垫资')))
               then 0
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1 and datediff(loan_time_xg, "2020-04-01") >= 0 then 1
            when bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl>=1  then obbh.fcsl
            when bao.product_name in ('限时贷','大道房抵贷') or (bao.product_type in ('保险类产品','现金类产品')  and obbh.fcsl=0)
               then 1  -- 和放款笔数折算不同
            when bao.product_name in ('大道易贷（贷款服务）','大道快贷（贷款服务）','及时贷（贷款服务）','大道按揭')
               then 0.5
            else 0
         end )
   end )  loan_num_xg -- 放款笔数销管
   ,(case
      when oofc.loan_time_xg is null then 0
      when bao.product_name in ('大道按揭', '保险服务') or (bao.apply_status_name='已终止' and (bco.OPINION_ like'人工操作失误%' or bco.OPINION_ like'变更产品%')) or (bao.product_type='现金类产品' and bao.relate_type_name = '到期更换平台') then 0
      when bao.product_type='现金类产品' then cfm.con_funds_cost
      when bao.product_name like'买付保%' then bfs.guarantee_amount
      else nvl(bnl.biz_loan_amount, 0) end ) release_amount_xg  -- 放款金额_销管   -- 20190725
   ,bao.isjms
   ,bim.arrival_time  -- 确认资金到账-到账时间
   ,obsu.account_  login_account_name -- 账号名
   ,t666.transfermark_time_min -- 确认放款时间
   ,tmb.main_borrower -- 主借款人
   ,case when bol.borrower_type = "PERSONAL" then "个人"
         when bol.borrower_type = "PUBLIC" then "公司"
   end borrower_type_name -- 原贷款借款人类型
   ,nvl(bol.busi_deduct_principal_balance, 0) + nvl(bol.fund_deduct_balance, 0) + nvl(bol.relation_loan_balance  , 0) total_loan_balance  -- 原贷本金余额
   ,nvl(trrf.is_relate_ransom_floor, "否") is_relate_ransom_floor -- 是否关联赎楼
   ,nvl(trh.in_reserve_house, "否") in_reserve_house -- 是否备用房
   ,nvl(bnl.biz_loan_amount, 0) + nvl(bnl.provident_fund_loan_amount, 0) total_loan_amount -- 新贷金额
   ,ocps.order_credit_parse_status  -- 征信解析状态
   ,ts.policy_name -- 策略代码
   ,toie.order_is_employed -- 自雇状态
   ,t666.investigate_time_min -- 审查时间
   ,nvl(bdi.actual_trading_price,bdi.trading_price) -- 实际成交价
   ,tmrdd.transaction_mortgage_time -- 办理抵押材料时间
   ,tmrdd.transaction_mortgage_user -- 办理抵押材料经办人
   ,tmrdd.transaction_loan_file_time -- 办理放款材料时间
   ,tmrdd.transaction_loan_file_user -- 办理放款材料经办人
   ,opi.agreeLoanResult_time -- 确认银行放款办理时间
   ,case when bao.version = '' or bao.version is null  then bim.arrival_time
         else bim.bank_loan_time
    end bank_loan_time-- 确认银行放款时间
   ,opi.agreeLoanResult_user -- 确认银行放款经办人
   ,bim.bank_loan_amount-- 确认银行放款金额
   ,tmrd.transfer_mark_user  -- 放款登记经办人
   ,tbhtd.new_house_no -- 新房产证号
   ,case when cfm.count_head_tail = "suanTouBuSuanWei" then "算头不算尾"
         when cfm.count_head_tail = "suanTouSuanWei" then "算头算尾"
    end count_head_tail  --费用算头尾方式
   ,t666.inputinfocomplete_time input_info_complete_time  -- 录入完成时间
   ,case when bao.man_check_first = 'Y' then '先审批后面签'
          when bao.man_check_first = 'N' then '先面签后审批'
    end man_check_first -- 是否先审批
   ,bdi.trading_price transfer_owner_trading_price  -- 过户价格
   ,case when bao.product_name = "大道按揭" then tqeaj.house_evaluation_company
         else tqehd.price_source
    end price_source -- 评估机构
   ,case when bol.has_second_mortgage = "Y" then "是"
         when bol.has_second_mortgage = "N" then "否"
    end has_second_mortgage -- 原贷是否二押
   ,case when bnl.third_type = 1 then '公司'
         when bnl.third_type = 2 then '个人'
    end third_type -- 新贷第三方类型
   ,bnl.new_loan_rate -- 新商贷利率
   ,cfm.con_funds_cost -- 确认资金到账-到账金额
   ,tplaf.query_time query_time_first_all-- 首次个诉查询时间(全文)
   ,tplaf.query_user_name query_user_first_all-- 首次个诉查询经办人(全文)
   ,tplof.query_time query_time_first_one-- 首次个诉查询时间(单篇)
   ,tplof.query_user_name query_user_first_one-- 首次个诉查询经办人(单篇)
   ,bao.follow_up_username -- 跟进人员名称
   ,bao.rob_user_name -- 跟单人
   ,opid.CheckResEnsure_opinion check_res_ensure_opinion -- 合作机构审批结果
   ,CASE WHEN bao.apply_status_name='已终止' THEN '终止'
    when fwh.warn_status=0 and (t666.overinsurance_time is null or t666.overinsurance_time = '' )then '预警'
    WHEN (bao.`apply_status_name`<>'已终止' and (fwh.warn_status<>0 or fwh.warn_status is null ) and  t666.overinsurance_time is NOT NULL)THEN '解保'
    ELSE '正常' END AS policy_state -- 保险状态
   ,nvl(bdum.osalesusername_min,bao.sales_user_name) osales_user_name -- 初始渠道经理
   ,bdu.datau_time osales_time -- 渠道经理最新变更时间
   ,nvl(bdu.nsalesusername,bao.sales_user_name) nsalesusername -- 最新渠道经理
   ,bim.start_time sendloancommand_start_time -- 发送放款指令时间
   ,tme.complete_time_ manual_end_time   -- 订单终止时间
   ,tc1.trans_day trans_time   -- 缴费录入提交时间
   ,ROW_NUMBER() OVER(PARTITION BY bao.apply_no ORDER BY bao.apply_time desc) rank
  ,case when bao.product_name = "大道按揭" then tpp.auxiliary_apply_no end auxiliary_apply_no -- 配套订单号
  ,case when bao.product_name = "大道按揭" then tpp.auxiliary_product_name end auxiliary_product_name -- 配套产品
   ,case when instr(bao.product_name,'大道按揭')>0 then bim.bank_loan_time else oofc.loan_time_xg end loan_time_yy -- 放款时间_运营
,case when bao.product_type='现金类产品' then cast (cfm.con_funds_cost as double)
   when bao.product_type='保险类产品' then cast (bnl.biz_loan_amount as double)
   when instr(bao.product_name,'买付保')>0 then cast ( nvl(tip1.insurance_amount,tip1.loan_amount) as double)
   when instr(bao.product_name,'大道快贷')>0 or instr(bao.product_name,'限时贷')>0 or instr(bao.product_name,'大道易贷')>0 or instr(bao.product_name,'拍卖保')>0 then cast (bnl.biz_loan_amount as double)
   when instr(bao.product_name,'大道按揭')>0 or instr(bao.product_name,'保险服务')>0 then cast (bnl.biz_loan_amount as double)
   else NULL end loan_amount_yy -- 放款金额_运营
,case when bao.product_type='现金类产品' then
t666.pushloancommand_time
when bao.product_type='保险类产品' then
t666.sendloancommand_time
end as sendloan_time -- 通知放款时间
,datediff((case when bao.product_type='现金类产品' then
t666.pushloancommand_time
when bao.product_type='保险类产品' then
t666.sendloancommand_time
end),brcj.remark_create_time_1) -- 加急天效
,brcj.remark_create_time_1 -- 最终加急时间
,brcx.remark_create_time_2 -- 最终销急时间
,t666.inputinfocomplete_time -- 最终录入完成时间
,tcc.inputinfo_create_time -- 最终录入创建时间
,tcc.investigate_create_time -- 最终审查创建时间
,tcc.inputinfocomplete_create_time -- 最终录入完成创建时间
,t666.inputinfo_time_min
,t666.inputinfo_user_name_min
,bfs.insured_amount_cardinality -- 保额计费基数
,bfs.insurance_premium_cardinality -- 保费计费基数
,bfs.charging_cardinality -- 收费计费基数
,tii1.insurer_name --投保人姓名
,tii1.insurer_cert_id -- 投保人证件号码
,bnl.total_loan_amount total_borrow_loan_amount -- 借款总金额
,tfi.paymensave_user_name -- 确认回款保存经办人
,bnl.agree_loan_source_name -- 同贷来源名称
,tst.market_remark -- 评估反馈
,bsr.create_time -- 评估创建日期
,bsr.house_evaluation_status -- 评估状态
,bsr.market_result -- 正评结果
,tcc.min_costmark_complete_time -- 最早费率登记时间
,t666.interview_is_outside_handle -- 是否外出面签
,boe.certificate_keep_name -- 产证保管
,t666.prerandom_is_outside_handle -- 是否外出预约赎楼
,brf.actual_repay_method_name -- 实际扣款方式
,t666.trustaccount_is_outside_handle -- 是否外出托管
,t666.queryarchive_is_outside_handle -- 是否外出查档
,t666.canclemortgage_is_outside_handle -- 是否外出注销抵押
,t666.mortgagepass_is_outside_handle -- 是否外出抵押递件
,t666.transferin_is_outside_handle -- 是否外出过户递件
,t666.transferout_is_outside_handle -- 是否外出过户出件
,te.test_channel_name -- 测试方式
,concat_ws(',',CASE WHEN bao.group_apply_no is not null and bao.group_apply_no<>'' then '联' END,case WHEN bim.is_special_approved='1' THEN '特' END,case WHEN bim.feedback_on_abnormal_order_flag='1' THEN '异' END,case WHEN bim.is_priority='Y' THEN '急' END,case WHEN (bao.thirdparty_name='PA' or bao.thirdparty_name='HZB' or bao.thirdparty_name='ZYB') THEN '对接' END)
as label -- 标签
,t666.returnconfirm_user_name -- 资金归还确认经办人
,t666.approvalresult_time -- 审批结果确认办理时间
,t666.approvalresult_user_name -- 审批结果确认经办人
,t666.inputinfocomplete_user_name -- 录入完成经办人
,tpel.VALUE -- 放款结果返回-放款时间
,bao.is_interview_name -- 是否需运营面谈
,bim.cooperative_guarantee_agency -- 合作担保机构
from ods_bpms_biz_apply_order_common bao
   LEFT JOIN ods_bpms_biz_fee_summary_common bfs on bfs.apply_no=bao.apply_no
   LEFT JOIN ods_bpms_biz_isr_mixed_common bim on bim.apply_no=bao.apply_no
   LEFT JOIN (select * from ods_bpms_biz_new_loan_common where rn=1) bnl on bnl.apply_no=bao.apply_no
   LEFT JOIN T tmp2 on tmp2.apply_no=bao.apply_no
   left join ods_bpms_sys_dic sd_pt on (bfs.`price_tag`=sd_pt.KEY_ and sd_pt.`TYPE_ID_`='10000042640043')
   left join tmp_org sys on sys.org_code=bao.sales_branch_id
   left join ods.ods_bpms_sys_org obso on obso.code_ = bao.sales_branch_id
   left join ods_bpms_biz_channel_common ban on ban.apply_no=bao.apply_no
   left join tmp_seel tseel on tseel.apply_no=bao.apply_no
   left join tmp_buy tbuy on tbuy.apply_no=bao.apply_no
   left join tmp_custormer bcr on bcr.apply_no=bao.apply_no
   left join temp_biz_house bh on bh.apply_no=bao.apply_no
   left join (select * from ods_bpms_biz_deal_info_common where rn=1) bdi on bdi.apply_no=bao.apply_no
   left join (select * from tmp_estimate tae where rn=1) tst on tst.apply_no=bao.apply_no
   left join (select * from tmp_query_aptitude where rn=1) bqa on bqa.apply_no=bao.apply_no
   left join (select * from tmp_credit tct where rn=1 )  tct1 on tct1.apply_no=bao.apply_no
   left join (select * from tmp_business tbn where rn=1 )  tbn1 on tbn1.apply_no=bao.apply_no
   left join (select * from tmp_personal_litigation tbp where rn=1 )  tbp on tbp.apply_no=bao.apply_no
   left join (select * from ods_bpms_biz_ori_loan_common where rn=1) bol on bol.apply_no=bao.apply_no
   left join tmp_supervision tsp on tsp.apply_no=bao.apply_no
   left join (select * from tmp_insurance_policy tip where rn=1 )  tip1 on tip1.apply_no=bao.apply_no
   left join (select * from tmp_insurance_info where rn=1 )  tii1 on tii1.apply_no=bao.apply_no
   left join (select * from tmp_archive_2) tah2 on tah2.apply_no=bao.apply_no
   left join tmp_personal_litigation_one tpl2 on tpl2.apply_no=bao.apply_no
   left join tmp_personal_litigation_all tpla2 on tpla2.apply_no=bao.apply_no
   left join (select * from tmp_test_record where rn=1) te on te.apply_no=bao.apply_no
   left join (select * from tmp_fd_advance where rn=1) fa on fa.apply_no=bao.apply_no
   left join ods_bpms_c_fund_module_common cfm on cfm.apply_no=bao.apply_no
   left join tmp_cost_trade tct on tct.apply_no=bao.apply_no
   left join tmp_cost_trade_all tcta on tcta.apply_no=bao.apply_no
   left join tmp_cost_trade_capital tctc on tctc.apply_no=bao.apply_no
   left join (select * from ods_bpms_biz_ransom_floor_common where rn=1 ) brf on brf.apply_no=bao.apply_no
   left join (select * from tmp_partner_grant where rn=1) tpg on tpg.apply_no=bao.apply_no
   left join tmp_fd_advance_fin tff on tff.apply_no=bao.apply_no
   left join (select * from tmp_fund_flow_info where rn=1) tfi on tfi.apply_no=bao.apply_no
   left join ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
   left join (
   select apply_no,
   concat_ws(',',collect_set(OPINION_))OPINION_
   from ods_bpms_bpm_check_opinion_common where `STATUS_`='manual_end' group by apply_no) bco on bco.apply_no=bao.apply_no
   left join (select * from tmp_cost  where rn=1 and trans_type = "CSC1") tc1 on tc1.apply_no=bao.apply_no
   left join (SELECT * from tmp_acount_1 where card_type = 'GLK' and rn=1) ta1_g on ta1_g.apply_no=bao.apply_no
   left join tmp_acount_1_close ta1_g1 on ta1_g1.apply_no=bao.apply_no
   left join (SELECT * from tmp_account ) ta1 on ta1.apply_no=bao.apply_no
   left join ods_bpms_order_fees_common_wx dwf on dwf.apply_no=bao.apply_no
   left join (select apply_no,NULL obtain_cert_expect_time from ods_bpms_biz_house_transfer where (apply_no<>'' and apply_no is not null) group by apply_no) bht on bht.apply_no=bao.apply_no
   left join tmp_opinion opi on bao.flow_instance_id = opi.PROC_INST_ID_
   left join tmp_opinion_desc opid on bao.flow_instance_id = opid.PROC_INST_ID_
   left join tmp_missing_materials tmm on tmm.apply_no = bao.apply_no
   left join tmp_qdrfar qdrfar on qdrfar.apply_no = bao.apply_no
   left join tmp_qdrwar qdrwar on qdrwar.apply_no = bao.apply_no
   left join (select obcct.apply_no, max(obcct.trans_day) max_cc01_time,max(b.fullname_) max_fullname, sum(obcct.trans_money) total_cc01_money from ods.ods_bpms_c_cost_trade obcct
   left join ods.ods_bpms_sys_user b on (b.id_=obcct.update_user_id)
   where obcct.trans_type = "CC01" group by obcct.apply_no) trade_cc01 on trade_cc01.apply_no = bao.apply_no
   left join (select bh.house_no, count(*) fcsl from ods.ods_bpms_biz_house bh
               where bh.house_type not in ("qt", "cw") and bh.house_no is not null and bh.house_no <> "" and bh.in_reserve_house <> "Y"
               group by bh.house_no ) obbh on obbh.house_no = bao.house_no
   left join ods_bpms_biz_apply_order_extend_common boe on boe.apply_no=bao.apply_no
   left join ods_bpms_sys_user obsu on bao.sales_user_id = obsu.id_
   left join tmp_main_borrower tmb on bao.apply_no = tmb.apply_no
   left join tmp_relate_ransom_floor trrf on bao.apply_no = trrf.apply_no
   left join tmp_reserve_house trh on trh.apply_no = bao.apply_no
   left join tmp_order_credit_parse_status ocps on bao.apply_no = ocps.apply_no
   left join tmp_strategy ts on bao.apply_no = ts.apply_no
   left join tmp_order_is_employed toie on bao.apply_no = toie.apply_no
   left join ods_bpms_biz_product_apply_info_common bpai on bpai.apply_no = bao.apply_no
   left join (select * from ods_bpms_biz_house_transfer_common tip where rn=1 )  bhtc on bhtc.apply_no=bao.apply_no
   left join tmp_matter_record tmrd on tmrd.apply_no = bao.apply_no
   left join tmp_matter_record_d tmrdd on tmrdd.apply_no = bao.apply_no
   left join tmp_biz_house_transfer_d tbhtd on tbhtd.house_no = bao.house_no
   left join tmp_query_estimate_h_d tqehd on tqehd.house_no = bao.house_no
   left join tmp_personal_litigation_one_first tplof on tplof.apply_no = bao.apply_no
   left join tmp_personal_litigation_all_first tplaf on tplaf.apply_no = bao.apply_no
   left join ods.ods_orders_finance_common oofc on oofc.apply_no = bao.apply_no
   left join (select * from tmp_warn_hist where rn=1)fwh on fwh.apply_no = bao.apply_no
   left join (select * from tmp_sales where rn=1)bdum on bdum.apply_no=bao.apply_no
   left join tmp_sales_max bdu on bdu.apply_no=bao.apply_no
   left join dim.dim_company dca on bao.branch_id = dca.company_id_3_level
   left join tmp_query_estimate_aj tqeaj on bao.house_no = tqeaj.house_no
 left join (select brc1.apply_no,brc1.create_time remark_create_time_1  from ods_bpms_biz_order_remark_common brc1 where rn=1
and content_tag='加急') brcj on brcj.apply_no=bao.apply_no
left join (select brc2.apply_no,brc2.create_time remark_create_time_2 from ods_bpms_biz_order_remark_common brc2 where rn=1 and content_tag='销急') brcx on  brcx.apply_no=bao.apply_no
left join tmp_checkcreate tcc on tcc.apply_no=bao.apply_no
left join (select * from temp_manual_end where rank =1) tme on bao.flow_instance_id = tme.PROC_INST_ID_
left join temp_biz_house bhg on bhg.apply_no=bao.group_apply_no
left join dwd.tmp_pt_product tpp on bao.apply_no = tpp.apply_no
left join tmp_estimate_record_history bsr on bsr.apply_no=bao.apply_no
left join (select * from tmp_p2p_extend_lst where rn = 1) tpel on tpel.apply_no = bao.apply_no;
drop table if exists dwd.dwd_orders_ex;
ALTER TABLE dwd.dwdtmp_orders_ex RENAME TO dwd.dwd_orders_ex;