drop table if exists dws.dwstmp_report_orders_jzzy;
CREATE TABLE dws.dwstmp_report_orders_jzzy (
	date_key bigint comment '日期代理键'
	,company_key bigint comment '公司代理键'
	,product_key bigint comment '产品代理键'
	,partner_insurance_name string comment '合作机构'
	,input_info_order_num double comment  '录入发生订单量'
 	,input_info_end_order_num double comment  '录入提交订单量'
 	,input_materials_order_num double comment  '录入补件数量'
 	,send_material_order_num double comment  '外传处理订单量'
 	,input_info_complete_order_num double comment  '录入完成处理订单量'
	,etl_update_time timestamp 
);

insert overwrite table dws.dwstmp_report_orders_jzzy
select
dd.id date_key -- '日期代理键'
,dc.s_key company_key -- '公司代理键'
,nvl(dp.id, 0) product_key -- '产品代理键'
,tdoa.partner_insurance_name -- '合作机构'
,sum(tdoa.input_info_order_num) input_info_order_num -- '录入发生订单量'
,sum(tdoa.input_info_end_order_num) input_info_end_order_num -- '录入提交订单量'
,sum(tdoa.input_materials_order_num) input_materials_order_num -- '录入补件数量'
,sum(tdoa.send_material_order_num) send_material_order_num -- '外传处理订单量'
,sum(tdoa.input_info_complete_order_num) input_info_complete_order_num -- '录入完成处理订单量'
,from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss') etl_update_time
from dws.tmp_dws_order_agg tdoa
left join dws.dimension_date dd on tdoa.report_date = dd.calendar
left join dim.dim_company dc on tdoa.branch_id = dc.company_id_3_level
left join dws.dimension_product dp on tdoa.product_name = dp.product_name
group by dd.id, dc.s_key, nvl(dp.id, 0), tdoa.partner_insurance_name
;

drop table if exists dws.dws_report_orders_jzzy;
ALTER TABLE dws.dwstmp_report_orders_jzzy RENAME TO dws.dws_report_orders_jzzy;
