SET hive.execution.engine=spark;
USE ods ;
DROP TABLE IF EXISTS dwd.tmpdwd_order_fenrun_ledger;
CREATE TABLE IF NOT EXISTS dwd.tmpdwd_order_fenrun_ledger (
  `cwgd_date_m` STRING COMMENT '财务归档月份',
  `cwgd_date` TIMESTAMP COMMENT '财务归档时间',
  `jyce_date` TIMESTAMP COMMENT '校验差额时间',
  `ywzt` STRING COMMENT '业务状态',
  `cwgd_user` STRING COMMENT '财务归档经办人',
  `city_name` STRING COMMENT '业务归属',
  `apply_no` STRING COMMENT '订单编号',
  `product_name` STRING COMMENT '业务品种',
  `product_type` STRING COMMENT '产品类型',
  `seller_name` STRING COMMENT '客户姓名',
  `partner_bank_name` STRING COMMENT '合作银行',
  `partner_insurance_name` STRING COMMENT '保险公司',
  `slje` DOUBLE COMMENT '赎楼金额/周转金额',
  `fkjd` STRING COMMENT '放款节点',
  `product_term_and_charge_way` STRING COMMENT '收费方式',
  `product_term` bigint comment '客户收费期限',
  `risk_grade` STRING COMMENT '风险标签',
  `borrowing_amount` DOUBLE COMMENT '借款金额（元）',
  `mq_date` TIMESTAMP COMMENT '面签时间',
  `fk_date` TIMESTAMP COMMENT '放款日期',
  `sjgh_date` TIMESTAMP COMMENT '实际归还日期',
  `dzsjykts` bigint comment '垫资实际用款天数',
  `ptsjykts` bigint comment '平台实际用款天数',
  `sjykts` bigint comment '客户实际用款天数',
  `jflr_ss` DOUBLE COMMENT '缴费录入实收合计',
  `jflr_jm` DOUBLE COMMENT '缴费录入减免合计',
  `jflr_ys` DOUBLE COMMENT '缴费录入应收合计',
  `fy_ys` DOUBLE COMMENT '返佣（应收）',
  `fy_ss` DOUBLE COMMENT '返佣报表里面的实返总额',
  `sfzq` STRING COMMENT '收费正确',
  `frje` DOUBLE COMMENT '已分润',
  `chengben` DOUBLE COMMENT '保费/资金成本（元）',
  `pyjtf_ys` DOUBLE COMMENT '偏远交通费（元）应收',
  `fr1_ysj_zb` DOUBLE COMMENT '分润1：应收计（元）-总部',
  `fr1_ysj_jms` DOUBLE COMMENT '分润1：应收计（元）-加盟商',
  `fr2_ysj_zb` DOUBLE COMMENT '分润2：实收计（元）-总部',
  `fr2_ysj_jms` DOUBLE COMMENT '分润2：实收计（元）-加盟商',
  `company_name_2_level` STRING COMMENT '2级公司',
  `profit_exceed` DOUBLE COMMENT '校验差额',
  `etl_update_time` TIMESTAMP COMMENT '更新时间',
  `wce` DOUBLE COMMENT '误差额（元）',
  `frje_zb` DOUBLE COMMENT '本次分润金额（元）-总部',
  `frje_jms` DOUBLE COMMENT '本次分润金额（元）-加盟商',
  `remark` STRING COMMENT '备注'
);

WITH report_taizhang_caiwu_fanyong AS (
SELECT
ibof.apply_no , -- 订单编号

(nvl((sum_humanpool_rebate),0)
+ nvl((sum_replace_turn_out), 0)
+ nvl((sum_invoice_rebate), 0)
) shifan_amount -- 实返金额

FROM (SELECT * FROM ods.ods_bpms_biz_apply_order WHERE apply_no !='') ibof
LEFT JOIN (SELECT apply_no,SUM(nvl(humanpool_rebate,0))AS sum_humanpool_rebate FROM ods.ods_bpms_biz_ledger_pay GROUP BY apply_no)blp ON blp.apply_no = ibof.apply_no
LEFT JOIN (SELECT apply_no,SUM(nvl(replace_turn_out,0))AS sum_replace_turn_out FROM ods.ods_bpms_biz_ledger_instead GROUP BY apply_no)bli ON bli.apply_no = ibof.apply_no
LEFT JOIN (SELECT apply_no,SUM(nvl(invoice_rebate,0))AS sum_invoice_rebate FROM ods.ods_bpms_biz_ledger_invoice GROUP BY apply_no)blin ON blin.apply_no = ibof.apply_no
)
,t_fact_finacial_table AS (
SELECT src2.rownum_asc,src1.rownum_desc,src3.*
FROM (SELECT id,apply_no,trans_type,
row_number() over(PARTITION BY apply_no,trans_type ORDER BY trans_day DESC) rownum_desc
FROM ods.ods_bpms_c_cost_trade
WHERE trans_type IS NOT NULL) src1
LEFT JOIN (SELECT id,apply_no,trans_type,
row_number() over(PARTITION BY apply_no,trans_type ORDER BY id ASC) rownum_asc
FROM ods.ods_bpms_c_cost_trade
WHERE trans_type IS NOT NULL)src2 ON (src1.id=src2.id)
JOIN (SELECT t1.`id`,t1.apply_no ,t3.`NAME_` `settl_way`,t1.`trans_type`, t1.`trans_money`,
t1.`trans_day`, t1.`payer_acct`,t1.`payer_acct_no`, t1.`payer_bank_name`,t1.`payee_acct`,
t1.`payee_acct_no`,t1.`payee_bank_name`,t1.`trade_status`
FROM ods.ods_bpms_c_cost_trade t1
LEFT JOIN ods.ods_bpms_sys_dic t3 ON (t1.`settl_way`=t3.`KEY_` AND t3.`TYPE_ID_`='10000010570024')
ORDER BY t1.apply_no,t1.trans_type,t1.`id` ASC ) src3 ON (src1.id=src3.id)
 )
,temp_t_fact_finacial_table AS ( -- ttfft
SELECT apply_no,
SUM(CASE WHEN tfft.trans_type='CSD1' AND tfft.trade_status = '1' THEN nvl(tfft.trans_money,0)END)  trans_money ,
SUM(CASE WHEN tfft.trans_type='CSC1' THEN nvl(tfft.trans_money,0)END)  AS jiaofei_amt
FROM t_fact_finacial_table tfft GROUP BY apply_no
)
,ids_fee_items_v2 AS (
SELECT t.*
FROM (SELECT t1.`apply_no`,
MAX((CASE WHEN t.`fee_define_no` IN ('totalReceivableFee', 'feeTotal') THEN t.`fee_value` ELSE NULL END))  AS totalReceivableFee_fee_value
FROM  ods.ods_bpms_biz_apply_order_common t1
LEFT JOIN ods.ods_bpms_biz_fee_detial t ON (t1.`apply_no`=t.`apply_no`)
WHERE t1.product_version NOT IN ("v1.0", "v1.5")
GROUP BY t1.`apply_no`) t
)
,temp_c_charge_detail AS ( -- tccd
SELECT apply_no,
SUM(nvl(ccd.pre_collect,0)) AS pre_collect

FROM ods.ods_bpms_c_charge_detail ccd
WHERE ccd.charge_type = "reduction"
GROUP BY apply_no
)
,tmp AS (
SELECT
t.apply_no,
nvl(t12.totalReceivableFee_fee_value,0)  - nvl((tccd.pre_collect),0) AS total_receivable,
nvl(ttfft.jiaofei_amt, 0) - nvl(ttfft.trans_money, 0) AS total_collection
FROM dwd.dwd_order_finance t
LEFT JOIN ids_fee_items_v2 t12 ON t.apply_no = t12.apply_no
LEFT JOIN temp_c_charge_detail tccd ON t.apply_no = tccd.apply_no
LEFT JOIN temp_t_fact_finacial_table ttfft ON t.apply_no = ttfft.apply_no
)
,temp_cash_jms_rate AS (SELECT t.apply_no,opfr.jms_rate,opfr.dd_rate
FROM (SELECT apply_no,branch_name,guidang_date,financial_time,
	CASE WHEN product_type='保险类产品' THEN '保险类'
	WHEN product_type='现金类产品' THEN '现金类'
	ELSE product_type END product_type
	FROM dwd.`dwd_order_finance`) t
LEFT JOIN dim.dim_order_fr_rate opfr ON opfr.product_type= t.product_type
		AND t.branch_name = TRIM(opfr.jms_name)
WHERE nvl(t.guidang_date,t.financial_time) >= opfr.start_date AND nvl(t.guidang_date,t.financial_time) < opfr.end_date
)
,temp_insur_jms_rate AS (SELECT t.apply_no,opfr.jms_rate,opfr.dd_rate
FROM (SELECT apply_no,branch_name,guidang_date,financial_time,
	CASE WHEN product_type='保险类产品' THEN '保险类'
	WHEN product_type='现金类产品' THEN '现金类'
	ELSE product_type END product_type
	FROM dwd.`dwd_order_finance`) t
LEFT JOIN dim.dim_order_fr_rate opfr ON opfr.product_type= t.product_type
		AND t.branch_name = TRIM(opfr.jms_name)
WHERE (t.guidang_date) >= opfr.start_date AND (t.guidang_date) < opfr.end_date
)

INSERT overwrite TABLE dwd.`tmpdwd_order_fenrun_ledger`

SELECT
date_format(nvl(a.cwgd_date,a.jyce_date), "yyyyMM") cwgd_date_m,
a.*,
CAST(date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss')AS TIMESTAMP)   etl_update_time,
a.fr2_ysj_zb-a.fr1_ysj_zb wce, --  误差额（元）   ：  分润2实收总部-分润1应收总部
a.fr2_ysj_zb frje_zb, --  本次分润金额（元）-总部
ROUND(nvl(a.fr2_ysj_jms-frje, 0), 2) frje_jms, --  本次分润金额（元）-加盟商
'' remark --  备注

FROM 
(
SELECT 
t.guidang_date cwgd_date
,t.financial_time jyce_date
,t.apply_status   ywzt
,t.guidang_user cwgd_user
,t.branch_name city_name
,t.apply_no
,t.product_name
,t.product_type
,doe.seller_name -- 	seller_name_all
,t.partner_bank_name  -- '合作银行'
,t.partner_insurance_name
,NULL slje
,NULL  fkjd -- '放款节点'
,t.product_term_and_charge_way
,t.fixed_term product_term
,t.risk_grade
,shenqing_amount borrowing_amount
,interview_date mq_date
,t.loan_time_jc fk_date
,return_date sjgh_date
,nvl(DATEDIFF(t.floorAdvance_ret_date,t.floorAdvance_fin_date), 0) + nvl(DATEDIFF(t.expireAdvance_ret_date,t.expireAdvance_fin_date), 0) dzsjykts
,DATEDIFF(t.return_date,t.platform_value_date)  ptsjykts -- '平台实际用款天数'
,t.fund_use_date_num sjykts -- '客户实际用款天数'
,ROUND(t.actual_amount_total, 2) jflr_ss -- '缴费录入实收合计'
,ROUND(t.reduction_amount, 2) jflr_jm -- '缴费录入减免合计'
,ROUND(t.zsf_ys, 2) jflr_ys -- '缴费录入应收合计'
,nvl(bfd.min_fee_value,0) fy_ys -- '返佣（应收）'
,ROUND(rtcf.shifan_amount, 2)  fy_ss -- '返佣报表里面的实返总额'
,IF((CEIL(t.actual_amount_total) - CEIL(t.zsf_ys)) >= 0
	   ,"正确" ,"错误") sfzq -- '收费正确'
,nvl(ROUND(t.profits_money, 2), 0) frje -- '已分润'
,nvl(ROUND(occf.cost_capital, 2), 0) chengben -- 资金成本
,nvl(t.pyjt_amount, 0) pyjtf_ys
,ROUND((
		nvl(t.zsf_ys, 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(occf.cost_capital, 0)
		-nvl((bfd.min_fee_value), 0)
		)
		*nvl(CAST(opfr.dd_rate AS DOUBLE), 0)
		+nvl(t.pyjt_amount, 0)
	   ,2)AS  fr1_ysj_zb -- '分润1：应收计（元）-总部'
,ROUND((
		nvl(t.zsf_ys, 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(occf.cost_capital, 0)
		-nvl((bfd.min_fee_value), 0)
		)
		*nvl(CAST(opfr.jms_rate AS DOUBLE), 0)
	  ,2) AS fr1_ysj_jms -- '分润1：应收计（元）-加盟商 '
,ROUND((
		nvl(t.actual_amount_total, 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(occf.cost_capital, 0)
		-nvl(ROUND(rtcf.shifan_amount, 2), 0)
		)
		*nvl(CAST(opfr.dd_rate AS DOUBLE), 0)
		+nvl(t.pyjt_amount, 0)
		, 2) AS fr2_ysj_zb -- '分润2：实收计（元）-总部'
,ROUND((
		nvl(t.actual_amount_total, 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(occf.cost_capital, 0)
		-nvl(ROUND(rtcf.shifan_amount, 2), 0)
		)
		*nvl(CAST(opfr.jms_rate AS DOUBLE)
				, 0)
		, 2) AS fr2_ysj_jms -- '分润2：实收计（元）-加盟商'
,nvl(dc.company_name_2_level, t.city_name) company_name_2_level -- '2级公司'
,if(t.profit_exceed <= 0 ,NULL,t.profit_exceed)
FROM dwd.`dwd_order_finance` t
LEFT JOIN dwd.dwd_orders_ex doe on t.apply_no=doe.apply_no
LEFT JOIN ods.ods_bpms_biz_apply_order_common bao ON t.apply_no = bao.apply_no
LEFT JOIN (SELECT MIN(fee_value)AS min_fee_value,apply_no FROM ods.ods_bpms_biz_fee_detial  WHERE LOWER(fee_define_no) LIKE "%rakebackfee%" GROUP BY apply_no)bfd ON bfd.apply_no = t.apply_no
LEFT JOIN report_taizhang_caiwu_fanyong rtcf ON rtcf.apply_no = t.apply_no
LEFT JOIN dim.dim_company dc ON t.city_name = dc.company_name_3_level
LEFT JOIN dwd.tmp_order_cost_capital_fr occf ON occf.apply_no = t.apply_no
LEFT JOIN temp_cash_jms_rate opfr on opfr.apply_no= t.apply_no
WHERE (t.guidang_date IS NOT NULL OR t.profit_exceed>0)
AND bao.service_type = "JMS" AND t.product_type != '保险类产品'
) AS a


UNION ALL 


SELECT 
date_format(a.cwgd_date, "yyyyMM") cwgd_date_m, -- 财务归档月份
a.*
,ROUND(a.fr2_ysj_zb-a.fr1_ysj_zb,2) wce -- 误差额（元）
,a.fr2_ysj_zb frje_zb -- 本次分润金额（元）-总部
,ROUND(nvl(a.fr2_ysj_jms-frje, 0), 2) frje_jms -- '本次分润金额（元）-加盟商 '
,'' remark -- '备注'

FROM
(
SELECT 
  t.guidang_date cwgd_date -- '财务归档时间'
  ,t.financial_time jyce_date
  ,t.apply_status ywzt -- '业务状态'
  ,t.guidang_user cwgd_user
  ,t.branch_name city_name -- '业务归属'
  ,t.apply_no -- '订单编号'
  ,t.product_name -- '业务品种'
  ,t.product_type
  ,doe.seller_name -- '客户姓名'
  ,t.partner_bank_name  -- '合作银行'
  ,t.partner_insurance_name  -- '保险公司'
  ,bnl.biz_loan_amount slje
  ,sd.NAME_  fkjd -- '放款节点'
  ,t.product_term_and_charge_way
  ,t.fixed_term product_term
  ,t.risk_grade
  ,shenqing_amount borrowing_amount
  ,t.interview_date mq_date -- '面签时间'
  ,doe.bank_loan_time fk_date -- '放款日期'
  ,NULL sjgh_date
  ,NULL dzsjykts
  ,NULL ptsjykts -- '平台实际用款天数'
  ,NULL sjykts -- '客户实际用款天数'
  ,nvl(tp.total_collection, 0) + nvl((cct_csc1.premium), 0)
	- nvl((cct_csd1.premium), 0)
	jflr_ss --  '缴费录入实收合计'
  ,ROUND(t.reduction_amount, 2) jflr_jm --  '缴费录入减免合计'
--  ,nvl(tp.total_receivable, 0) + nvl((bfd1.fee_value), 0) jflr_ys --  '缴费录入应收合计'
    ,nvl(tp.total_receivable, 0)
    + nvl((t.premium_amount), 0)
    jflr_ys --  '缴费录入应收合计'
  ,nvl(bfd.min_fee_value,0) fy_ys -- '返佣（应收）'
  ,ROUND(rtcf.shifan_amount, 2) fy_ss --  '返佣报表里面的实返总额'
  ,IF((CEIL(tp.total_collection) - CEIL(tp.total_receivable)) >= 0 
	   ,"正确" ,"错误") sfzq -- '收费正确'
  ,ROUND(nvl(t.profits_money,0), 2) frje -- '已分润'
--  ,bip.premium_amount chengben -- '保费成本（元）'
--  ,t.premium_amount chengben -- '保费成本（元）'
  ,nvl(t.actualinsurancepremium,nvl(bfdip.insurancePremium,bfdip.insuranceFee)) chengben
  ,nvl(t.pyjt_amount, 0) pyjtf_ys -- '偏远交通费（元）应收'
  ,ROUND((
		nvl(tp.total_receivable, 0) + nvl(bip.premium_amount,0) --nvl((t.premium_amount), 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(t.actualinsurancepremium,nvl(bfdip.insurancePremium,bfdip.insuranceFee)) --nvl(t.premium_amount, 0)
		-nvl((bfd.min_fee_value), 0)
		)
		*nvl(CAST(opfr.dd_rate AS DOUBLE), 0)
		+nvl(t.pyjt_amount, 0)  
	   ,2)AS  fr1_ysj_zb -- '分润1：应收计（元）-总部 '
  ,ROUND((
		nvl(tp.total_receivable, 0) + nvl(bip.premium_amount,0) --nvl((t.premium_amount), 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(t.actualinsurancepremium,nvl(bfdip.insurancePremium,bfdip.insuranceFee)) --nvl(t.premium_amount, 0)
		-nvl((bfd.min_fee_value), 0)
		)
		*nvl(CAST(opfr.jms_rate AS DOUBLE), 0)
	   ,2)AS  fr1_ysj_jms -- '分润1：应收计（元）-加盟商 '
  ,ROUND((
		nvl(tp.total_collection, 0) + nvl((cct.premium), 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(t.actualinsurancepremium,nvl(bfdip.insurancePremium,bfdip.insuranceFee)) --nvl(t.premium_amount, 0)
		-nvl((ROUND(rtcf.shifan_amount, 2)), 0)
		)
		*nvl(CAST(opfr.dd_rate AS DOUBLE), 0)
		+nvl(t.pyjt_amount, 0) 
		, 2) AS fr2_ysj_zb -- '分润2：实收计（元）-总部 '
  ,ROUND((
		nvl(tp.total_collection, 0) + nvl((cct.premium), 0)
		-nvl(t.pyjt_amount, 0)
		-nvl(t.actualinsurancepremium,nvl(bfdip.insurancePremium,bfdip.insuranceFee)) --nvl(t.premium_amount, 0)
		-nvl((ROUND(rtcf.shifan_amount, 2)), 0)
		)
		*nvl(CAST(opfr.jms_rate AS DOUBLE), 0)
		, 2) AS fr2_ysj_jms -- '分润2：实收计（元）-加盟商 '
  ,nvl(dc.company_name_2_level, t.city_name) company_name_2_level -- '2级公司'
  ,if(t.profit_exceed <= 0 ,NULL,t.profit_exceed)
  ,CAST(date_format(current_timestamp(),'yyyy-MM-dd HH:mm:ss')AS TIMESTAMP) etl_update_time

FROM dwd.dwd_order_finance t
-- LEFT JOIN (SELECT * FROM `biz_ransom_floor_last` WHERE rank =1)t5 ON (t.`apply_no`=t5.`apply_no`)  
-- LEFT JOIN ods_bpms_biz_fee_summary  t8 ON (t.`apply_no`=t8.`apply_no`) 
LEFT JOIN dwd.dwd_orders_ex doe on t.apply_no=doe.apply_no
LEFT JOIN dim.dim_company dc ON t.city_name = dc.company_name_3_level
LEFT JOIN ods.ods_bpms_biz_apply_order bao ON t.apply_no = bao.apply_no
LEFT JOIN (SELECT MIN(fee_value)AS min_fee_value,apply_no FROM ods.ods_bpms_biz_fee_detial  WHERE LOWER(fee_define_no) LIKE "%rakebackfee%" GROUP BY apply_no)bfd ON bfd.apply_no = t.apply_no
LEFT JOIN ods.ods_bpms_biz_isr_mixed bim ON t.apply_no = bim.apply_no
LEFT JOIN ods.ods_bpms_sys_dic sd ON bim.tail_release_node = sd.KEY_ AND sd.TYPE_ID_ = "10000047750219"
LEFT JOIN report_taizhang_caiwu_fanyong rtcf ON t.apply_no = rtcf.apply_no
LEFT JOIN (SELECT MAX(premium)AS premium,apply_no FROM ods.ods_bpms_c_cost_trade GROUP BY apply_no)cct ON t.apply_no = cct.apply_no
LEFT JOIN (SELECT SUM(nvl(premium,0))AS premium,apply_no FROM ods.ods_bpms_c_cost_trade WHERE trans_type = 'CSC1' GROUP BY apply_no)cct_csc1 ON t.apply_no = cct_csc1.apply_no
LEFT JOIN (SELECT SUM(nvl(premium,0))AS premium,apply_no FROM ods.ods_bpms_c_cost_trade WHERE trans_type = 'CSD1' AND trade_status = '1' GROUP BY apply_no)cct_csd1 ON t.apply_no = cct_csd1.apply_no
--LEFT JOIN (SELECT SUM(nvl(fee_value,0))AS fee_value,apply_no FROM ods_bpms_biz_fee_detial WHERE fee_define_no IN ("insuranceFee" , 'insurancePremium') GROUP BY apply_no)bfd1 ON bfd1.apply_no = t.apply_no
LEFT JOIN (SELECT MAX(premium_amount)AS premium_amount,apply_no FROM ods.ods_bpms_biz_insurance_policy GROUP BY apply_no)bip ON bip.apply_no = t.apply_no
LEFT JOIN temp_insur_jms_rate opfr ON opfr.apply_no = t.apply_no
LEFT JOIN tmp tp ON t.apply_no = tp.apply_no
LEFT JOIN ods.ods_bpms_biz_new_loan bnl ON t.apply_no = bnl.apply_no
LEFT JOIN (select apply_no
    ,sum(case when fee_define_no = "insurancePremium" then nvl(fee_value,0) end) insurancePremium -- 保费
    ,sum(case when fee_define_no = "insuranceFee" then nvl(fee_value,0) end) insuranceFee -- 保费
    from ods.ods_bpms_biz_fee_detial bfd group by apply_no)bfdip on t.apply_no=bfdip.apply_no
WHERE t.guidang_date IS NOT NULL AND bao.service_type = "JMS" 
AND t.product_type='保险类产品' AND LENGTH(bao.product_id)>11
)a
;

DROP TABLE IF EXISTS dwd.dwd_order_fenrun_ledger;
ALTER TABLE dwd.tmpdwd_order_fenrun_ledger RENAME TO dwd.dwd_order_fenrun_ledger;