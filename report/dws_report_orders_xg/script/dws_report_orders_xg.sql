set hive.execution.engine=spark;
drop table if exists dws.dwstmp_report_orders_xg;
CREATE TABLE dws.dwstmp_report_orders_xg (
	date_key string
	,org_key string
	,product_key string
	,new_order_num double comment "新增订单量"
	,interview_order_num double comment "面签订单量"
	,interview_xg_num double comment "面签笔数"
	,agreeloanmark_order_num double comment "同贷订单量"
	,loan_order_num double comment "放款订单量"
	,loan_xg_num double comment "放款笔数"
	,release_amount_xg double comment "放款金额"
	,manual_end_order_num double comment "退单量"
	,interview_noloan_num double comment "面签未放款订单"
	,loan_not_repaid_num double comment "回款在途订单数"
	,not_financial_archive_num double comment "未财务归档订单"
	,etl_update_time timestamp 
);

with 
-- 为了匹配不是渠道经理获取到的订单，并且去重
tmp_dim_employe_org as (
	select * 
	from(
		select 
		ad.user_id, ad.sub_department_id, ad.sub_company_id, ad.s_key
		,ROW_NUMBER() OVER(PARTITION BY ad.user_id, ad.sub_department_id, ad.sub_company_id ORDER BY ad.sub_department_id ) rk
		from dws.dim_employe_org ad
		where job_code not like "%qdjl%"
	) aa 
	where aa.rk = 1
),

-- 为了匹配没有部门id的订单
tmp_dim_employe_org3 as (
	select *
	from(
		select 
		ad.user_id, ad.sub_company_id, ad.s_key 
		,ROW_NUMBER() OVER(PARTITION BY ad.user_id, ad.sub_company_id ORDER BY ad.sub_company_id ) rk
		from dws.dim_employe_org ad
	) aa 
	where aa.rk = 1
)
insert overwrite table dws.dwstmp_report_orders_xg
select 
dd.id date_key
,nvl(nvl(nvl(du.s_key, du2.s_key), du3.s_key), 0) org_key
,ddt.id product_key
,sum(tdoa.new_order_num) new_order_num
,sum(tdoa.interview_order_num) interview_order_num
,sum(tdoa.interview_xg_num) interview_xg_num
,sum(tdoa.agreeloanmark_order_num) agreeloanmark_order_num
,sum(tdoa.loan_order_num) loan_order_num
,sum(tdoa.loan_xg_num) loan_xg_num
,sum(tdoa.release_amount_xg) release_amount_xg
,sum(tdoa.manual_end_order_num) manual_end_order_num
,sum(nvl(tdota.interview_noloan_num, 0)) interview_noloan_num
,sum(nvl(tdota.loan_not_repaid_num, 0)) loan_not_repaid_num
,sum(nvl(tdota.not_financial_archive_num, 0)) not_financial_archive_num
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from dws.tmp_dws_order_agg tdoa
left join dws.tmp_dws_order_total_agg tdota 
	on tdoa.report_date = tdota.report_date
	and tdoa.product_name = tdota.product_name
	and tdoa.sales_user_name = tdota.sales_user_name
	and tdoa.sales_user_id = tdota.sales_user_id
	and tdoa.sales_branch_id = tdota.sales_branch_id
	and tdoa.branch_id = tdota.branch_id
left join dws.dimension_date dd on tdoa.report_date = dd.calendar
left join dws.dimension_product ddt on tdoa.product_name = ddt.product_name
left join dws.dim_employe_org du 
	on tdoa.sales_user_id = du.user_id 
	and tdoa.sales_branch_id = du.sub_department_id 
	and tdoa.branch_id = du.sub_company_id
	and du.job_code like "%qdjl%"
left join tmp_dim_employe_org du2 
	on tdoa.sales_user_id = du2.user_id 
	and tdoa.sales_branch_id = du2.sub_department_id 
	and tdoa.branch_id = du2.sub_company_id
left join tmp_dim_employe_org3 du3
	on tdoa.sales_user_id = du3.user_id 
	and tdoa.branch_id = du3.sub_company_id
group by dd.id, nvl(nvl(nvl(du.s_key, du2.s_key), du3.s_key), 0), ddt.id
;

drop table if exists dws.dws_report_orders_xg;
ALTER TABLE dws.dwstmp_report_orders_xg RENAME TO dws.dws_report_orders_xg;

	
