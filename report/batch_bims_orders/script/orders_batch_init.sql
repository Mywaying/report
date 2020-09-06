use ods;
with tmp_customer as (select
bcr.apply_no,
group_concat(bcr.customer_no) customer_no,
group_concat(bcr.name) customer_name,
group_concat(bqc.credit_channel_name) credit_channel_name,
group_concat(bqc.parse_way) parse_way,
group_concat(bcr.id_card_no) id_card_no,
group_concat(bcr.sex) sex,
group_concat(cast (bcr.age as string)) age,
group_concat(bcr.phone) phone,
group_concat(bcr.income_type_name) income_type_name,
group_concat(bcr.customer_type_name) customer_type_name,
group_concat(bcr.individual_type_name) individual_type_name,
group_concat(bcr.company_name) company_name,
group_concat(bcr.marital_status_name) marital_status_name,
group_concat(bcr.marital_status_tag) marital_status_tag,
group_concat(from_timestamp(bqc.parse_ret_time,'yyyy-MM-dd HH:mm:ss')) parse_ret_time,
group_concat(bqc.credit_type) credit_type
from
(select * from ods_bims_biz_customer_rel_common bcr where
instr(upper(bcr.relation),'JKR_GUARANTY')>0 and (bcr.apply_no is not null
and bcr.apply_no<>'')and delete_flag = '0') bcr
left join ods_bims_biz_query_credit_common bqc on bcr.customer_no=bqc.customer_no
GROUP BY bcr.apply_no),
tmp_mortgage_orgnization as (
select
bcr.house_no,
cast(group_concat(cast (bcr.mortgage_amount as string))as double) mortgage_amount,
group_concat(cast (bcr.mortgage_credit_amount as string)) mortgage_credit_amount,
group_concat(cast (bcr.mortgage_payway  as string)) mortgage_payway,
group_concat(cast (bcr.mortgage_term  as string)) mortgage_term,
group_concat(cast (bcr.is_mortgage  as string)) is_mortgage,
group_concat(cast (bcr.mortgage_way  as string)) mortgage_way
from ods_bims_biz_house_mortgage_orgnization bcr
GROUP BY house_no
),
tmp_query_estimate_wx as (
select t.apply_no,
max((case when lower(t.`price_source`)='fxt' then t.`price` else null end)) fxt_market_average_price ,
max((case when lower(t.`price_source`)='sl' then t.`price` else null end)) sl_market_average_price ,
max((case when lower(t.`price_source`)='yf' then t.`price` else null end)) yf_market_average_price ,
max((case when lower(t.`price_source`)='personal' then t.`price` else null end))personal_market_average_price ,
max((case when lower(t.`price_source`)='oth' then t.`price` else null end))oth_market_average_pricefrom from ods_bims_biz_query_estimate_common t
group by t.apply_no
),
tmp_personal_litigation as(
select apply_no,
type, -- 类型-one：单篇；all：全文
`name`,
query_time, --  个人诉讼查询时间
query_user_name, --  个人诉讼查询经办人
hass_unfinish_exec, --有无未完结执行信息
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY query_time asc) rn
from (
select a.apply_no,a.customer_no,a.name,b.type,b.query_time,b.query_user_name,b.hass_unfinish_exec from ods_bims_biz_customer_rel_common a
left join ods_bims_biz_personal_litigation b on b.customer_no=a.customer_no
)T
),
tmp_personal_litigation_one as (
select T.*,ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time asc) rn from
(select nvl(tct.apply_no,bbl.apply_no) apply_no,bbl.hass_unfinish_exec,tct.customer_no,bbl.customer_name,bbl.create_time from ods_bims_biz_personal_litigation bbl left join
ods_bims_biz_customer_rel_common tct on tct.customer_no=bbl.customer_no
where bbl.type='ALL' and hass_unfinish_exec='Y')T
),
tmp_manual_end as (
SELECT apply_no,TASK_NAME_,opinion_,complete_time_,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time_ asc) rn
from ods_bims_bpm_check_opinion_common_ex
where status_='manual_end'),
tmp_option as (
SELECT apply_no,
TASK_NAME_,
opinion_,
complete_time_,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY complete_time_ desc) rn
from ods_bims_bpm_check_opinion_common_ex
where status_<>'manual_end'  and complete_time_ is not null
and cast(complete_time_ as string)<>'' and task_key_<>'endNode'),
app as (
select * from ods_app_biz_order where apply_no not in (select thirdparty_no from ods_bims_biz_apply_order)
)
,tmp_org as (
select
org_code,
org_name_, -- 所属团队
group_concat(fullname_) org_leader, -- 团队长_市场主管
group_concat(mobile_) mobile -- 团队长_市场主管
from ods_bims_sys_org_user_common  where user_post like '%市场%主管%'
and status_=1
group by org_code,org_name_
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
  trans_type ORDER BY 	trans_day asc) rn
from ods_bims_c_cost_trade a left join ods_bims_sys_user b on a.create_user_id = b.id_
),
tmp_t_manage as (
select
organization_no,
organization_name, -- 所属团队
group_concat(name) org_leader, -- 团队长_市场主管
group_concat(mobile_no) org_mobile -- 团队长
from ods_app_t_manager where team_leader='1'
group by organization_no,organization_name
),tmp_insurance_info as (
select apply_no,
is_pass,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from ods_bims_biz_insurance_policy
)
insert overwrite table dwd.dwd_bims_order
SELECT
bao.apply_no 'apply_no' -- 业务编号
, bao.branch_name 'city' -- 所属城市
, bao.thirdparty_no 'thirdparty_no' -- 小程序编号
,bao.product_name -- 产品名称
,case when lower(bhc.mortgage_status_name)='是' then bao.product_type_name else
  '一押' end 'product_type' -- 业务品种
, bao.partner_insurance_name 'partner_insurance_name' -- 合作机构
, bao.partner_bank_name 'partner_bank_name' -- 保险机构
, bao.apply_status_name 'apply_status_name' -- 订单状态
, case when bao.thirdparty_name='system' then '线下' else '线上' end source_type  -- 进件方式
, bqc.credit_channel_name 'credit_channel' -- 进件征信获取方式
, bqc.credit_channel_name 'sign_channel' -- 备签征信获取方式
, bqc.credit_channel_name 'borrower_channel' -- 借款人征信获取方式
, qew.fxt_market_average_price 'evaluateprice_fxt' -- 出额房讯通估价
, qew.sl_market_average_price 'evaluateprice_sl' -- 出额世联估价
, qew.yf_market_average_price 'evaluateprice_rf' -- 出额云房估价
, bhc.comprehensive_average_price 'evaluateprice' -- 出额综合评估价
, qew.personal_market_average_price 'evaluateprice_personal' -- 人工评估价
, bhc.comprehensive_evaluation_price 'evaluateprice_last' -- 最终评估价
, bao.loan_amount 'loan_amount' -- 出额额度
, bai.intent_apply_amount 'intent_apply_amount' -- 意向申请金额
, bim.final_apply_loan_amount 'final_apply_loan_amount' -- 终审额度
, bao.loan_amount 'borrower_amount' -- 放款额度
, bao.loan_amount/bhc.comprehensive_evaluation_price 'loan_amount_level' -- 出额成数
,case when bhc.comprehensive_evaluation_price>0 then bai.intent_apply_amount/bhc.comprehensive_evaluation_price
else 0 end 'intent_apply_amount_level' -- 意向申请成数
,case when bhc.comprehensive_evaluation_price>0 then bai.intent_apply_amount/bhc.comprehensive_evaluation_price
else 0 end 'last_level' -- 终审成数
,case when bhc.comprehensive_evaluation_price>0 then bim.final_apply_loan_amount/bhc.comprehensive_evaluation_price
else 0 end 'loan_level' -- -- 放款成数
, bdr.`describe` 'Initial_result' -- 初评结果
, bdr.`describe` 'loan_result' -- 出额结果
,t666.submitapproval_status_name 'shadow_result' -- 影像审批结果
, regexp_replace(bdri.`describe`,'SUCCESS','成功') 'last_result' -- 终审结果
, case when t666.sendverifymaterial_option='同意' then '是'
  else
    case upper(bii.is_pass) when 'Y' then '是' when 'N' then '否' end
  end 'underwriting_result' -- 核保结果
, bpw.team_org_approve_result 'capital_result' -- 资方审批结果
, bpw.loan_result 'loan_result_1' -- 放款结果
, case when cast(nvl(bdr.investigateflag, '0') as bigint )>0 then '是' else '否' end 'is_vestigate' -- 是否下户
, case when cast(nvl(bdr.investigateflag, '0') as bigint )>=2 then '是' else '否' end 'is_manage' -- 是否经营下户
, qdr.remark 'refusereason' -- 拒绝原因
, tme.TASK_NAME_ 'aborttask' -- 终止节点
, bao.remark 'abortreason' -- 终止原因
, bao.seller_name 'seller_name' -- 借款人
, bqc.`id_card_no` 'seller_id_card_no' -- 证件号码
, regexp_replace(regexp_replace(lower(bqc.`sex`),'m','男'),'f','女') 'seller_sex' -- 性别
, bqc.`age` 'seller_age' -- 年龄
, bqc.`phone` 'seller_phone' -- 联系方式
, bqc.`income_type_name` 'seller_income_type' -- 客户类型
, bqc.`individual_type_name` 'seller_individual_type' -- 个人类型
, bqc.`company_name` 'seller_company_name' -- 工作单位
, bqc.`marital_status_name` 'seller_marital_status' -- 婚姻状况
, case when upper(tplo.hass_unfinish_exec)='Y' then '是' else '否' end 'is_litigation' -- 个人诉讼全文
, bhc.house_cert_no 'house_cert_no' -- 房产编号
, bhc.house_location_province_name 'house_location_province' -- 房产证省
, bhc.house_location_city_name 'house_location_city' -- 房产证市
, bhc.house_area_code_name 'house_location_area' -- 房产证区
, bhc.house_address 'house_address' -- 房产详细地址
, bhc.house_property_name 'house_property' -- 房产性质
, cast(bhc.house_age as double) 'house_age' -- 房龄
, cast(bhc.house_acreage as double) 'house_acreage' -- 面积
, bhc.rent_status_name 'rent_status' -- 出租情况
,bhc.mortgage_status_name --抵押状态
, mo.mortgage_amount 'mortgage_amount' -- 在押余额
, mo.mortgage_credit_amount 'mortgage_credit_amount' -- 在押授信金额
, regexp_replace(regexp_replace(regexp_replace(regexp_replace(mo.mortgage_payway,'debj','等额本金'),'debx','等额本息'),'xxhb','先息后本'),'debx','等额本息') 'mortgage_payway' -- 在押还款方式
, mo.mortgage_term 'mortgage_term' -- 在押期限
, bhc.loan_agency_name 'loan_agency_name' -- 贷款机构名称
, regexp_replace(regexp_replace(regexp_replace(bhc.loan_agency_type,'bank','银行'),'nonBank','非银行'),'individuality','个人') 'loan_agency_type' -- 贷款机构类型
, regexp_replace(regexp_replace(mo.is_mortgage,'1','是'),'0','否') 'is_mortgage' -- 是否按揭
, bai.repay_method 'repay_method' -- 还款方式
, bai.loan_usage 'loan_usage' -- 贷款用途
, qdrc.remark 'interview_comment' -- 备注信息
, tmo.TASK_NAME_ 'current_task' -- 当前节点
, NULL 'evaluateprice_bdfxt' -- 报单房讯通估价
, NULL 'evaluateprice_bdsl' -- 报单世联估价
, NULL 'evaluateprice_bdyf' -- 报单云房估价
, NULL 'evaluateprice_bdrf' -- 报单综合评估价
, NULL 'isransom' -- 是否赎楼
, NULL 'credit_channel_advice' -- 报单阶段征信解析结果
, NULL 'is_hit' -- 借款人第三方数据
,sys.org_leader 'org_leader' -- 团队长
,bao.sales_user_name 'sales_user_name' -- 渠道经理
,sys.mobile 'chanel_mobile' -- 渠道经理电话
,bch.contact 'chanel_contact' -- 渠道姓名
,bch.channel_type_name 'channel_type_name' -- 渠道类型
,bch.channel_name 'channel_name' -- 渠道名称
,bch.channel_phone 'channel_phone' -- 渠道联系人电话
,t666.householdapprove_user_name 'householdapprove_user_name' -- 下户经办人
,t666.interview_user_name 'interview_user_name' -- 面签人员
,tc1.user_name 'user_name' -- 缴费录入人员
,bfw.agentcommissionperorder_fee_value 'agentcommissionperorder_fee_value' -- 代收返佣（%）
,t666.mortgagepass_user_name 'mortgagepass_user_name' -- 抵押递件经办人
,t666.mortgageout_user_name 'mortgageout_user_name' -- 抵押出件经办人
,t666.sendverifymaterial_user_name 'sendverifymaterial_user_name' -- 核保资料外传经办人
,t666.insuranceinfoconfirm_user_name 'insuranceinfoconfirm_user_name' -- 保单确认经办人
,t666.checkmarsend_user_name 'checkmarsend_user_name' -- 审批资料外传经办人
,t666.pushloancommand_user_name 'pushloancommand_user_name' -- 放款指令推送经办人
,t666.sendloancommand_user_name 'sendloancommand_user_name' -- 出账审核经办人
,bai.repay_method_name 'repay_method_name' -- 还款方式
,bai.loan_rate 'loan_rate' -- 贷款年利率%
,bai.loan_term 'loan_term' -- 贷款期限
,bfs.borrowing_value_date 'borrowing_value_date' -- 起息日
,bfs.borrowing_due_date 'borrowing_due_date' -- 到期日
,CASE bao.partner_insurance_id WHEN 'XTD-ZY001' THEN '10' WHEN 'XTB-ZX001' THEN '15' WHEN 'XTB-AJ001' THEN '20' END 'partner_insurance_date' -- 每月还款日
,brp.currentstatus_name 'currentstatus_name' -- 当期状态
,case when bdrup.apply_no is null then '否' else '是' end  'upload_pictures_flag' -- 出额是否补件
,case when bora.apply_no is null then '否' else '是' end 'approvesupplywaiting_flag' -- 影像审批是否补件
,case when bdrsu.apply_no is null then '否' else '是' end 'supplement_data_flag' -- 大数审批是否补件
,case when borw.apply_no is null then '否' else '是' end 'accountchecksupplywaiting_flag' -- 出账审核是否补件
,t666.applyorder_time 'applyorder_time' -- 报单时间
,t666.quota_time 'quota_time' -- 出额时间
,t666.confirmationloan_time 'confirmationloan_time' -- 确认贷款意向时间
,t666.inputapproveinfo_time 'inputapproveinfo_time' -- 初审信息录入时间
,t666.householdapprove_time 'householdapprove_time' -- 下户时间
,t666.submitapproval_time 'submitapproval_time' -- 影像资料确认时间
,t666.approvalimage_time 'approvalimage_time' -- 人工初审时间
,t666.interview_time 'interview_time' -- 面签时间
,t666.approvalresult_time 'approvalresult_time' -- 完成终审时间
,t666.sendverifymaterial_time 'sendverifymaterial_time' -- 核保结果返回时间
,t666.insuranceinfoconfirm_time 'insuranceinfoconfirm_time' -- 保单信息确认时间
,bpw.team_org_approve_create_time 'team_org_approve_create_time' -- 资方审批外传时间
,t666.checkmarsend_time 'checkmarsend_time' -- 资方审批结果确认时间
,t666.mortgagepass_time 'mortgagepass_time' -- 抵押递件时间
,t666.pushloancommand_time 'pushloancommand_time' -- 放款指令推送时间
,t666.sendloancommand_time 'sendloancommand_time' -- 发送放款指令时间
,bim.bank_loan_time 'bank_loan_time' -- 放款时间
,t666.mortgageout_time 'mortgageout_time' -- 抵押出件时间
,t666.prefile_time 'prefile_time' -- 归档时间
,bao.sales_branch_name 'sales_branch_anme' -- '所在市场团队'
,WeekOfYear(t666.applyorder_time) 'weeknum' -- '进件周数'
,NULL  'areatype'-- '区域类型'
,bdr.risklevel 'grade'-- '评级'
,bqc.parse_ret_time 'creditback_time' -- '业务系统征信返回时间'
,IF(LOWER(bqc.credit_type)='complete','解析成功',NULL) 'credit_advice' -- '业务系统征信解析结果'
,t666.quota_user_name 'quota_user_name' -- '出额经办业务助理姓名'
,cast(bdr.calcreditamt as double) 'quota_amount' -- '出额金额'
,bdr.remark 'quota_refusereason' -- '出额否决原因'
,if(t666.submitapproval_status_name='同意',null,t666.submitapproval_option) 'submitapproval_refusereason' -- '影像审批拒绝原因'
,qdrc.`describe` 'interview_result' -- '面签审批结果'
,qdrc.rejectreason  'interview_refusereason' -- '面签审批拒绝原因'
,t666.submitverifyapply_time 'submitverify_apply_time' -- '提交核保申请结束时间'
,tme.complete_time_ 'aborttime' -- '退单时间'
,t666.sendverifymaterial_time 'insurance_approval_time' -- '保方审批结果返回时间'
,cast(bdr.calcreditamt as double) 'loanable_amount' -- '可贷额度'
,brc.update_date 'icreditback_time' -- '大数i房征信返回时间'
,brc.credit_result_explain 'icredit_advice' -- '大数i房征信解析结果'
,bo.apply_status_name 'is_access' -- '是否准入'
,brc.rulename 'i_refusereason' -- '大数i房否决原因'
,bim.bank_loan_amount 'bank_loan_amount' --'放款金额'
,qdrc.update_time 'interview_result_back_time'-- '面签审批结果返回时间'
,bdr.investigateflag_name  'householdtype'--'下户类型'
,bcc.capital_name  'contracting_agency' --'签约机构'
,bo.create_date 'i_applyorder_time' --'大数i房提交时间'
,case bh.primary_product  when '030' then '一押'  when '100' then '二押' end 'i_product_name' --'i申请产品'
,bo.organization_city_name 'i_city' --'i城市'
,bo.manager_name 'i_sales_user_name' --'i客户经理姓名'
,te.organization_name 'i_sales_branch_anme' --'i所在市场团队'
,te.org_leader 'i_org_leader' --'i团队长姓名'
,WEEKOFYEAR(bo.create_date) 'i_weeknum' --'i进件周数'
,CONCAT(bh.house_city_name,bh.house_area_name)'i_house_location' --'i房产证地址（区）'
,NULL 'i_areatype' --'i区域类型'
,cast(CAST(YEAR(NOW())AS bigint)-CAST(bh.age AS bigint)as double) 'i_house_age' --'i房龄'
,cast (round(bh.area,2) as double) 'i_house_acreage' --'i建筑面积'
,bh.evaluate_price 'i_evaluateprice' --'i综合评估价'
,bo.apply_amount / bh.evaluate_price 'i_loan_amount_level' --'i成数'
,NULL 'i_grade' --'i评级'
,(CASE bh.primary_product WHEN '030' THEN bh.evaluate_price * bo.apply_amount / bh.evaluate_price
  WHEN '100' THEN CAST((bh.evaluate_price * bo.apply_amount / bh.evaluate_price) AS FLOAT) - CAST(nvl(bh.mortgage_amount_remain,0) AS FLOAT) END) 'i_loanable_amount' --'i可贷额度'
,(CASE WHEN bao.product_type="0" AND mo.mortgage_way="ybdy" THEN (cast(bdr.calcreditamt as double)+mo.mortgage_amount)/ bhc.comprehensive_evaluation_price
    WHEN bao.product_type="0" AND mo.mortgage_way="zgedy" THEN (cast(bdr.calcreditamt as double)+CAST(nvl(mo.mortgage_credit_amount,'0') AS DOUBLE))/ bhc.comprehensive_evaluation_price
    ELSE NULL END) 'level'--'成数'
from ods_bims_biz_apply_order_common bao
    left join tmp_customer bqc on bqc.apply_no=bao.apply_no
left join (select * from ods_bims_biz_house_common bqc where rn=1) bhc on bhc.apply_no=bao.apply_no
left join ods_bims_biz_product_apply_info_common bai on bai.apply_no=bao.apply_no
left join ods_bims_biz_isr_mixed_common bim on bim.apply_no=bao.apply_no
left join (select * from ods_bims_biz_query_docking_result_common bdr where bdr.bus_type='DY020' and rn=1 ) bdr on bdr.apply_no=bao.apply_no
left join (select * from ods_bims_biz_query_docking_result_common bdr where bdr.bus_type='INTERVIEW' and rn=1) bdri on bdri.apply_no=bao.apply_no
left join (select * from tmp_insurance_info where rn=1) bii on bii.apply_no=bao.apply_no
left join (select apply_no, remark from ods_bims_biz_query_docking_result_common where `describe`='拒绝' and rn=1) qdr on qdr.apply_no=bao.apply_no
left join tmp_mortgage_orgnization mo on mo.house_no=bhc.house_no
left join (select apply_no, remark,`describe`,rejectreason,update_time from ods_bims_biz_query_docking_result_common where bus_type='QUERY_INTERVIEW' and rn=1) qdrc on qdrc.apply_no=bao.apply_no
left join ods_bims_biz_p2p_ret_common_wx bpw on bpw.apply_no=bao.apply_no
left join tmp_query_estimate_wx qew on qew.apply_no=bao.apply_no
left join (select * from tmp_personal_litigation_one where rn=1) tplo on tplo.apply_no=bao.apply_no
left join (select * from tmp_manual_end where rn=1) tme on tme.apply_no=bao.apply_no
left join (select * from tmp_option where rn=1) tmo on tmo.apply_no=bao.apply_no
left join tmp_org sys on sys.org_code=bao.sales_branch_id
left join (select apply_no,currentstatus_name from ods_bims_biz_repayment_plan_common where rn=1) brp on brp.apply_no=bao.apply_no
left join (select * from tmp_cost  where rn=1 and trans_type = "CSC1") tc1 on tc1.apply_no=bao.apply_no
left join ods_bims_order_fees_common_wx bfw on bfw.apply_no=bao.apply_no
left join (select apply_no from ods_bims_biz_query_docking_result_common bdr where bdr.bus_type='UPLOAD_PICTURES' and rn=1) bdrup on bdrup.apply_no=bao.apply_no
left join ods_bims_biz_channel_common bch on bch.apply_no=bao.apply_no
left join ods_bims_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
left join ods_bims_biz_fee_summary bfs on bfs.apply_no=bao.apply_no
left join (select apply_no from ods_bims_biz_query_docking_result_common bdr where bdr.bus_type='SUPPLEMENT_DATA' and rn=1) bdrsu on bdrsu.apply_no=bao.apply_no
left join (select * from ods_bims_biz_order_matter_record_common where rn=1 and status_ ='ApproveSupplyWaiting') bora on bora.apply_no=bao.apply_no
left join (select * from ods_bims_biz_order_matter_record_common where rn=1 and status_ ='AccountCheckSupplyWaiting') borw on borw.apply_no=bao.apply_no
-- left join (select * from ods_bims_biz_query_docking_result_common bdr where bdr.bus_type='DY030' and bdr.node_key= "SubmitApproval") bdrsa on bdrsa.apply_no=bao.apply_no
left join ods_bims_b_cooperate_capital bcc ON bcc.capital_code=bai.contract_agency
left join ods_app_biz_order bo on bo.apply_no = bao.thirdparty_no
left join ods_app_biz_house bh on bh.apply_no=bo.apply_no
left join ods_app_biz_customer bc on bc.apply_no=bo.apply_no
left join ods_app_biz_risk_control_common brc on brc.apply_no=bo.apply_no
left join tmp_t_manage te on te.organization_no=bo.organization_no
union all
select
NULL --业务编号
,bo.organization_city_name --所属城市
,bo.apply_no --小程序编号
,'房抵贷' --
,case bh.primary_product  when '030' then '一押'  when '100' then '二押' end --业务品种
,NULL --签约机构
,NULL --保险机构
,bo.apply_status_name --订单状态
,NULL --进件方式
,NULL --进件征信获取方式
,NULL --备签征信获取方式
,case brc.credit_type  when 'report' then '上传征信报告'  when 'zld' then '中兰德征信查询认证码' end --借款人征信获取方式
,NULL --出额房讯通估价
,NULL --出额世联估价
,NULL --出额云房估价
,NULL --出额综合评估价
,NULL --人工评估价
,bh.evaluate_price --最终评估价
,bh.price --出额额度
,NULL --意向申请金额
,NULL --终审额度
,NULL --放款额度
,NULL --出额成数
,NULL --意向申请成数
,NULL --终审成数
,NULL --放款成数
,regexp_replace(regexp_replace(brc.judge_result,'P','通过'),'D','不通过') --初评结果
,regexp_replace(regexp_replace(brc.judge_result,'P','通过'),'D','不通过') --出额结果
,NULL --影像审批结果
,NULL --终审结果
,NULL --核保结果
,NULL --资方审批结果
,NULL --放款结果
,NULL --是否下户
,NULL --是否经营下户
,NULL --拒绝原因
,NULL --终止节点
,NULL --终止原因
,bc.name --借款人姓名
,NULL --借款人证件号码
,case cast (substr(bc.cert_no,17,1) as bigint)%2 when 0 then '男'  when 1 then '女' else '未知' end --借款人性别
,cast (cast (year(current_timestamp()) as bigint)- cast (substr(bc.cert_no,7,4) as bigint) as string) --借款人年龄
,bc.phone --借款人联系方式
,NULL --客户类型
,NULL --个人类型
,NULL --工作单位
,NULL --婚姻状况
,NULL --个人诉讼全文
,NULL --房产编号
,bh.house_province_name --房产证省
,bh.house_city_name --房产证市
,bh.house_area_name	 --房产证区
,bh.house_address --房产详细地址
,NULL --房产性质
,cast(CAST(YEAR(NOW())AS bigint)-CAST(bh.age AS bigint)as double) --房龄
,cast (round(bh.area,2) as double) --面积
,NULL --出租情况
,CASE lower(bh.is_mortgage) WHEN 'y' THEN '是' WHEN 'n' THEN '否' WHEN '0' THEN '否'  WHEN '1' THEN '是' end --抵押状态
,bh.mortgage_amount_remain  --在押余额
,NULL --在押授信金额
,NULL --在押还款方式
,NULL --在押期限
,NULL --贷款机构名称
,NULL --贷款机构类型
,NULL --是否按揭
,NULL --还款方式
,NULL --贷款用途
,NULL --备注信息
,'未进入业务系统' --当前节点
, NULL 'evaluateprice_bdfxt' -- 报单房讯通估价
, NULL 'evaluateprice_bdsl' -- 报单世联估价
, NULL 'evaluateprice_bdyf' -- 报单云房估价
, NULL 'evaluateprice_bdrf' -- 报单综合评估价
, NULL 'isransom' -- 是否赎楼
, NULL 'credit_channel_advice' -- 报单阶段征信解析结果
, NULL 'is_hit' -- 借款人第三方数据
,te.org_leader -- '团队长'
,bo.manager_name -- '渠道经理'
,bo.manager_phone -- '渠道经理电话'
,bo.agent_name -- '渠道姓名'
,bo.channel_type -- '渠道类型'
,bo.channel_name -- '渠道名称'
,bo.agent_phone -- '渠道联系人电话'
,NULL -- '下户经办人'
,NULL -- '面签人员'
,NULL -- '缴费录入人员'
,NULL -- '代收返佣（%）'
,NULL -- '抵押递件经办人'
,NULL -- '抵押出件经办人'
,NULL -- '核保资料外传经办人'
,NULL -- '保单确认经办人'
,NULL -- '审批资料外传经办人'
,NULL -- '放款指令推送经办人'
,NULL -- '出账审核经办人'
,NULL -- '还款方式'
,NULL -- '贷款年利率%'
,NULL -- '贷款期限'
,NULL -- '起息日'
,NULL -- '到期日'
,NULL -- '每月还款日'
,NULL -- '当期状态'
,NULL -- '出额是否补件'
,NULL -- '影像审批是否补件'
,NULL -- '大数审批是否补件'
,NULL -- '出账审核是否补件'
,bo.create_date -- '报单时间'
,NULL -- '出额时间'
,NULL -- '确认贷款意向时间'
,NULL -- '初审信息录入时间'
,NULL -- '下户时间'
,NULL -- '影像资料确认时间'
,NULL -- '人工初审时间'
,NULL -- '面签时间'
,NULL -- '完成终审时间'
,NULL -- '核保结果返回时间'
,NULL -- '保单信息确认时间'
,NULL -- '资方审批外传时间'
,NULL -- '资方审批结果确认时间'
,NULL -- '抵押递件时间'
,NULL -- '放款指令推送时间'
,NULL -- '发送放款指令时间'
,NULL -- '放款时间'
,NULL -- '抵押出件时间'
,NULL -- '归档时间'
,te.organization_name -- '所在市场团队'
,WEEKOFYEAR(bo.create_date) -- '进件周数'
,NULL -- '区域类型'
,NULL -- '评级'
,NULL -- '业务系统征信返回时间'
,NULL -- '业务系统征信解析结果'
,NULL -- '出额经办业务助理姓名'
,NULL -- '出额金额'
,NULL -- '出额否决原因'
,NULL -- '影像审批拒绝原因'
,NULL -- '面签审批结果'
,NULL -- '面签审批拒绝原因'
,NULL -- '提交核保申请结束时间'
,NULL -- '退单时间'
,NULL -- '保方审批结果返回时间'
,(CASE bh.primary_product WHEN '030' THEN bh.evaluate_price * bo.apply_amount / bh.evaluate_price
  WHEN '100' THEN CAST((bh.evaluate_price * bo.apply_amount / bh.evaluate_price) AS FLOAT) - CAST(bh.mortgage_amount_remain AS FLOAT) END) -- '可贷额度'
,brc.update_date -- '大数i房征信返回时间'
,brc.credit_result_explain -- '大数i房征信解析结果'
,bo.apply_status_name -- '是否准入'
,brc.rulename -- '大数i房否决原因'
,NULL --'放款金额'
,NULL -- '面签审批结果返回时间'
,NULL  --'下户类型'
,NULL  --'签约机构'
,bo.create_date 'i_applyorder_time' --'大数i房提交时间'
,case bh.primary_product  when '030' then '一押'  when '100' then '二押' end 'i_product_name' --'i申请产品'
,bo.organization_city_name 'i_city' --'i城市'
,bo.manager_name 'i_sales_user_name' --'i客户经理姓名'
,te.organization_name 'i_sales_branch_anme' --'i所在市场团队'
,te.org_leader 'i_org_leader' --'i团队长姓名'
,WEEKOFYEAR(bo.create_date) 'i_weeknum' --'i进件周数'
,CONCAT(bh.house_city_name,bh.house_area_name)'i_house_location' --'i房产证地址（区）'
,NULL 'i_areatype' --'i区域类型'
,cast(CAST(YEAR(NOW())AS bigint)-CAST(bh.age AS bigint)as double) 'i_house_age' --'i房龄
,cast (round(bh.area,2) as double) 'i_house_acreage' --'i建筑面积'
,bh.evaluate_price 'i_evaluateprice' --'i综合评估价'
,bo.apply_amount / bh.evaluate_price 'i_loan_amount_level' --'i成数'
,NULL --'i评级'
,(CASE bh.primary_product WHEN '030' THEN bh.evaluate_price * bo.apply_amount / bh.evaluate_price
  WHEN '100' THEN CAST((bh.evaluate_price * bo.apply_amount / bh.evaluate_price) AS FLOAT) - CAST(bh.mortgage_amount_remain AS FLOAT) END) 'i_loanable_amount' --'i可贷额
,NULL --'成数'
from app bo left join ods_app_biz_house bh
on bh.apply_no=bo.apply_no
left join ods_app_biz_customer bc on bc.apply_no=bo.apply_no
left join ods_app_biz_risk_control_common brc on brc.apply_no=bo.apply_no
left join tmp_t_manage te on te.organization_no=bo.organization_no;