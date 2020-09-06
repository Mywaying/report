use ods ;
create table if not exists dwd.dwd_orders_repay_record (
apply_no STRING COMMENT '订单编号',
category STRING COMMENT '类别',
report_date timestamp COMMENT '汇总日期',
product_name STRING COMMENT '产品名称',
borrowing_amount double COMMENT '借款金额',
platform_value_date timestamp COMMENT '(平台)借款起息日',
platform_due_date timestamp COMMENT '(平台)借款到期日',
fixed_term double COMMENT '预估用款期限',
product_term double COMMENT '产品期限',
house_type STRING COMMENT '房屋类型',
product_term_and_charge_way_name STRING COMMENT '收费方式',
channelpricetotal_fee_value double COMMENT '渠道价费率',
commissionfeeratetotal_fee_value double COMMENT '代收返佣费率',
overduerateperday_fee_value double COMMENT '超期费率',
contractfeeratetotal_fee_value double COMMENT '合同费率合计',
city_name STRING COMMENT '分公司',
branch_name STRING COMMENT '子公司',
main_borrower_name STRING COMMENT '主借款人姓名',
main_borrower_card STRING COMMENT '身份证',
fund_package_name STRING COMMENT '资金类型',
charge_name STRING COMMENT '(客户)借款-收款户名',
charge_open_bank_name STRING COMMENT '(客户)借款-收款银行',
charge_number STRING COMMENT '(客户)借款-收款账号',
partner_insurance_name STRING COMMENT '合作保险/合作机构',
contract_start_date timestamp COMMENT '借款合同起始日',
contract_end_date timestamp COMMENT '借款合同到期日',
contract_no STRING COMMENT '借款合同',
sendloancommand_time timestamp COMMENT '发送放款指令时间',
sales_branch_name STRING COMMENT '市场部',
sales_user_name STRING COMMENT '渠道经理',
returnconfirm_time TIMESTAMP COMMENT '资金归还确认时间',
repaymentresult_time TIMESTAMP COMMENT '资金归还核销时间',
loan_return_time TIMESTAMP COMMENT '资金归还时间',
fact_xf DOUBLE COMMENT '实还息费',
fact_bx DOUBLE COMMENT '实还本息',
interview_time TIMESTAMP COMMENT '面签时间'
) STORED AS parquet;
with tmp_house as (
SELECT house_no,name_,ROW_NUMBER() OVER(PARTITION BY house_no ORDER BY tmp.create_time asc) rn
FROM  ods_bpms_biz_house tmp
join  ods_bpms_sys_dic dic on tmp.house_type = dic.key_ AND dic.type_id_ = '10000028440005'
where house_no is NOT NULL and house_no<>''
),
tmp_fee_detail as(
select t.`apply_no`,
max((case when lower(t.`fee_define_no`)='channelpricetotal' then t.`fee_value` else null end)) channelpricetotal_fee_value ,
max((case when lower(t.`fee_define_no`)='commissionfeeratetotal' then t.`fee_value` else null end)) commissionfeeratetotal_fee_value ,
max((case when lower(t.`fee_define_no`)='overduerateperday' then t.`fee_value` else null end)) overduerateperday_fee_value ,
max((case when lower(t.`fee_define_no`)='contractfeeratetotal' then t.`fee_value` else null end)) contractfeeratetotal_fee_value 
from  ods_bpms_biz_fee_detial t group by t.`apply_no`
),
tmp_sys_org as 
(
    SELECT 
    org1.CODE_ code_
    ,org2.NAME_ name_
    FROM ` ods_bpms_sys_org` org1
    left join  ods_bpms_sys_org org2 on org1.PARENT_ID_ = org2.ID_
    where org1.GRADE_ = '3'
    union 
    SELECT 
    org1.CODE_ code_
    ,org1.NAME_ name_
    FROM ` ods_bpms_sys_org` org1
    left join  ods_bpms_sys_org org2 on org1.PARENT_ID_ = org2.ID_
    where org1.GRADE_ = '2'
),
tmp_charge_account as (
select apply_no,
charge_name,   --  划回收款方(公司账号) 失败
charge_open_bank_name,
charge_number,
ROW_NUMBER() OVER(PARTITION BY apply_no ORDER BY create_time desc) rn
from  ods_bpms_biz_charge_account
),
tmp_p2p as (
select apply_no,
task_name,
create_time   contract_start_date, -- 借款合同起始日
ROW_NUMBER() OVER(PARTITION BY apply_no,task_name ORDER BY create_time desc) rn
from  ods_bpms_biz_p2p_ret
),
tmp_record as (
select t2.apply_no,
MAX((CASE WHEN lower(t2.matter_key)='sendloancommand'  THEN t2.handle_time ELSE NULL END))  sendloancommand_time,
MAX((CASE WHEN lower(t2.matter_key)='returnconfirm'  THEN t2.handle_time ELSE NULL END))  returnconfirm_time,
MAX((CASE WHEN lower(t2.matter_key)='sendloancommand'  THEN t2.remark ELSE NULL END))  sendloancommand_remark
from
ods_bpms_biz_order_matter_record_common t2 join
(select apply_no,matter_key_new,max(rn) rn from ods_bpms_biz_order_matter_record_common
where handle_time>='${var:logdate}' and handle_time < date_add('${var:logdate}',1)
group by apply_no,matter_key_new) b on b.apply_no=t2.apply_no and b.rn=t2.rn and b.matter_key_new=t2.matter_key_new
group by t2.apply_no
),
tmp_record_intervew as (
select t2.apply_no,
MAX((CASE WHEN lower(t2.matter_key)='interview'  THEN t2.handle_time ELSE NULL END))  interview_time
from  ods_bpms_biz_order_matter_record_common t2
left join tmp_record t on t2.apply_no = t.apply_no
where t2.handle_time< t.sendloancommand_time
GROUP BY t2.`apply_no`
),
tmp_p2p_extend as (
select apply_no,
`key`,
create_time   create_time, -- 借款合同起始日
ROW_NUMBER() OVER(PARTITION BY apply_no,`key` ORDER BY create_time desc) rn
from ods_bpms_biz_p2p_ret_extend
)
insert into dwd.`dwd_orders_repay_record`
select
bao.apply_no -- 订单编号
,'sendloancommand'
,cast ('${var:logdate}' as timestamp)
,bao.product_name -- 产品名称
,bfs.borrowing_amount -- 借款金额
,bfs.platform_value_date-- （平台）借款起息日
,bfs.platform_due_date-- （平台）借款到期日
,bfs.fixed_term -- 预估用款期限
,cast (bfs.product_term as double) -- 产品期限
,th.house_type -- 房屋类型 -- 房产用途
,bfs.product_term_and_charge_way_name -- 收费方式
,tfd.channelpricetotal_fee_value -- 渠道价费率
,tfd.commissionfeeratetotal_fee_value -- 代收返佣费率
,tfd.overduerateperday_fee_value -- 超期费率
,tfd.contractfeeratetotal_fee_value -- 合同费率合计
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(so.name_,'公司',''),'无锡','南外'),'苏州','南外'),'合肥','南外'),'南通（南京）事业部','南外'),'南外分','南外') city_name -- 分公司
,regexp_replace(so.name_,'南通（南京）事业部','南外') branch_name -- 子公司
,bc.`name` main_borrower_name -- 主借款人姓名
,bc.id_card_no main_borrower_card -- 身份证
,case when blfp.fund_package_name='直营' and lower(bao.service_type)='jms' then '加盟' else blfp.fund_package_name end fund_package_name -- 资金类型
,tcg.charge_name -- （客户）借款-收款户名
,tcg.charge_open_bank_name -- （客户）借款-收款银行
,tcg.charge_number -- （客户）借款-收款账号
,replace(bao.partner_insurance_name,' ','') partner_insurance_name -- 合作保险/合作机构
,tp.contract_start_date contract_start_date -- 借款合同起始日
,date_add(tp.contract_start_date , cast (bfs.product_term as bigint)) contract_end_date -- 借款合同到期日
,ifnull(bpe.VALUE,bir.contract_no) contract_no -- 借款合同
,t666.sendloancommand_time sendloancommand_time
,bao.sales_branch_name sales_org_name -- 市场部a
,bao.sales_user_name --渠道经理
,NULL -- 资金归还确认时间
,tpg.create_time repaymentresult_time -- 资金归还核销时间
,case when ( instr(bao.partner_insurance_name,'云南信托')>0 or instr(bao.partner_insurance_name,'湖南信托-主动管理')>0 or instr(bao.partner_insurance_name,'宝乾小贷')>0 ) then
tpg.create_time else t666.returnconfirm_time end loan_return_time -- 资金归还时间
,ifnull(cfm.fact_re_interest, 0) + ifnull(cfm.fact_re_penalty, 0) fact_xf-- 实还息费
,ifnull(cfm.fact_re_interest, 0) + ifnull(cfm.fact_re_penalty, 0) + ifnull(bfs.borrowing_amount, 0) fact_bx-- 实还本息  实还息费+放款本金(借款金额)
,tri.interview_time
from ods_bpms_biz_apply_order_common bao
left join ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no = bao.apply_no
left join (select bfs.*,tmp.name_ product_term_and_charge_way_name from  ods_bpms_biz_fee_summary bfs
left join  ods_bpms_sys_dic tmp on  tmp.key_ = bfs.product_term_and_charge_way
WHERE tmp.type_id_ = '10000045260203') bfs on bao.apply_no = bfs.apply_no
left join (select house_no,name_ house_type from tmp_house where rn=1) th on th.house_no=bao.house_no
left join tmp_fee_detail tfd on tfd.apply_no=bao.apply_no
left join (select apply_no,is_actual_borrower_name,customer_no from ods_bpms_biz_customer_rel where is_actual_borrower_name = 'Y') bcr on bao.apply_no = bcr.apply_no
left join  ods_bpms_biz_customer bc on bcr.customer_no = bc.cust_no
left join  ods_bpms_biz_lm_fund_package blfp on bfs.fund_package_code = blfp.fund_package_code
left join (select * from tmp_charge_account where rn=1) tcg on tcg.apply_no=bao.apply_no
left join (select * from tmp_p2p where rn=1 and task_name = 'TEAM_ORG_APPROVE') tp on tp.apply_no=bao.apply_no
left join (SELECT apply_no,contract_no FROM  ods_bpms_biz_isr_mixed) bir on bir.apply_no=bao.apply_no
left join (SELECT apply_no,VALUE FROM  ods_bpms_biz_p2p_ret_extend tmp WHERE tmp.`key` = 'contractNo') bpe on bpe.apply_no=bao.apply_no
left join  tmp_sys_org so on so.CODE_=bao.branch_id
left join (select * from tmp_p2p_extend where rn=1 and `key` = 'repaymentResult') tpg on tpg.apply_no=bao.apply_no
left join ods_bpms_c_fund_module_common cfm on bao.apply_no = cfm.apply_no
left join tmp_record_intervew tri on tri.apply_no = bao.apply_no
where NOT EXISTS(select 1 from dwd.dwd_orders_repay_record where report_date='${var:logdate}' and category='sendloancommand') and sendLoancommand_time>='${var:logdate}' and sendLoancommand_time < date_add('${var:logdate}',1)
and sendLoancommand_option='同意' and sendLoancommand_status_name='同意' and bao.apply_status_name not in ('已终止', '拒绝');
