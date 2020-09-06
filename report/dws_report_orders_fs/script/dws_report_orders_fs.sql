drop table if exists dws.dwstmp_report_orders_fs;
CREATE TABLE dws.dwstmp_report_orders_fs (
	date_key bigint comment "日期代理键",
	product_key bigint comment "产品代理键" ,
	company_key bigint comment "公司代理键",
	sc_user_name string comment "审查人员",
	partner_bank_name string comment "合作银行",
	apply_check_num double comment "报审量",
	first_check_argee_num double comment "一审通过量",
	send_back_missing_materials_num double comment "退补件量",
	end_check_argee_num double comment "终审通过量",
	first_check_reject_num double comment "一审驳回量",
	first_check_missing_materials_num double comment "一审补件量",
	sys_auto_dispose_num double comment "系统自动处理量",
	manual_dispose_num double comment "人工审查业务量",
	sys_auto_end_num double comment "系统清除订单量",
	investigate_num double comment "审查量",
	high_level_approval_num double comment "高阶审批量",
	high_level_approval_reject_num double comment "高阶审批终止量",
	high_level_approval_return_num double comment "高阶审批退回量",
	etl_update_time timestamp 
);

insert overwrite table dws.dwstmp_report_orders_fs
select 
dd.id date_key
,dp.id product_key
,dc.s_key company_key 
,sc_user_name -- "审查人员"
,partner_bank_name -- "合作银行"
,sum(apply_check_num) apply_check_num -- "报审量"
,sum(first_check_argee_num) first_check_argee_num -- "一审通过量"
,sum(send_back_missing_materials_num) send_back_missing_materials_num -- "退补件量"
,sum(end_check_argee_num) end_check_argee_num -- "终审通过量"
,sum(first_check_reject_num) first_check_reject_num -- "一审驳回量"
,sum(first_check_missing_materials_num) first_check_missing_materials_num -- "一审补件量"
,sum(sys_auto_dispose_num) sys_auto_dispose_num -- "系统自动处理量"
,sum(manual_dispose_num) manual_dispose_num -- "人工审查业务量"
,sum(sys_auto_end_num) sys_auto_end_num -- "系统清除订单量"
,sum(investigate_num) investigate_num -- "审查量"
,sum(high_level_approval_num) high_level_approval_num -- "高阶审批量"
,sum(high_level_approval_reject_num) high_level_approval_reject_num -- "高阶审批终止量"
,sum(high_level_approval_return_num) high_level_approval_return_num -- "高阶审批退回量"
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from dws.tmp_dws_risk_funnel_agg tdrfa
left join dws.dimension_date dd on tdrfa.report_date = dd.calendar
left join dws.dimension_product dp on tdrfa.product_name = dp.product_name
left join dim.dim_company dc on tdrfa.branch_id = dc.company_id_3_level
group by dd.id, dp.id, dc.s_key, sc_user_name, partner_bank_name 
;

drop table if exists dws.dws_report_orders_fs;
ALTER TABLE dws.dwstmp_report_orders_fs RENAME TO dws.dws_report_orders_fs;