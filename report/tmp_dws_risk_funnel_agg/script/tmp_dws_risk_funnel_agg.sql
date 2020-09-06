set hive.execution.engine=spark;
drop table if exists dws.tmp_dws_risk_funnel_agg;
CREATE TABLE dws.tmp_dws_risk_funnel_agg (
	report_date timestamp comment "日期",
	branch_id string comment "子公司id",
	sc_user_name string comment "审查人员",
	product_name string comment "产品名称",
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
insert overwrite table dws.tmp_dws_risk_funnel_agg
select 
to_date(p1.baodan_date) report_date
,p1.branch_id
,p1.sc_user_name
,p1.product_name
,p1.partner_bank_name
,nvl(count(p1.apply_no), 0) apply_check_num -- 报审量
,nvl(sum(case when p1.sc_status = "agree" and p1.is_missing_materials = "否" then 1 end), 0) first_check_argee_num -- 一审通过量
,nvl(count(p1.apply_no), 0)
 - nvl(sum(case when p1.sc_status = "agree" and p1.is_missing_materials = "否" then 1 end), 0) send_back_missing_materials_num -- 退补件量
,nvl(sum(case when p1.end_approval_status = "同意" then 1 end), 0) end_check_argee_num -- 终审通过量
,nvl(sum(case when p1.sc_status = "reject" and p1.is_missing_materials = "否" then 1 end), 0) first_check_reject_num -- 一审驳回量
,nvl(sum(case when p1.is_missing_materials = "是" then 1 end), 0) first_check_missing_materials_num -- 一审补件量
,nvl(sum(case when p1.sc_user_name = "系统自动审批" or (p1.sc_user_name = "系统自动终止" and p1.sc_status = "manual_end") then 1 end), 0) sys_auto_dispose_num -- 系统自动处理量
,nvl(count(p1.apply_no), 0)
 - nvl(sum(case when p1.sc_user_name = "系统自动审批" or (p1.sc_user_name = "系统自动终止" and p1.sc_status = "manual_end") then 1 end), 0) manual_dispose_num -- 人工审查业务量
,nvl(sum(case when p1.sc_status = "sys_auto_end" then 1 end), 0) sys_auto_end_num -- 系统清除订单量
,nvl(sum(case when p1.sc_date is not null then 1 end), 0) investigate_num -- 审查量
,nvl(sum(case when (p1.sc_user_name <> p1.sp_user_name) and (drfsul.sp_user_name is not null) then 1 end), 0) high_level_approval_num -- 高阶审批量
,nvl(sum(case when (p1.sc_user_name <> p1.sp_user_name and p1.end_approval_status = "人工终止") and (drfsul.sp_user_name is not null) then 1 end), 0) high_level_approval_reject_num -- 高阶审批终止量
,nvl(sum(case when p1.first_mancheck_status = "驳回" then 1 end), 0) high_level_approval_return_num -- 高阶审批退回量
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from dws.tmp_dws_risk_funnel_agg_p1 p1
left join dim.dim_risk_funnel_sp_user_list drfsul on drfsul.sp_user_name = p1.sp_user_name
group by to_date(p1.baodan_date), p1.branch_id, p1.sc_user_name, p1.product_name, p1.partner_bank_name;