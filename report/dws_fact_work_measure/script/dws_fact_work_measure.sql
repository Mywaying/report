set hive.execution.engine=spark;
drop table if exists dws.dws_fact_work_measure;
create table if not exists dws.dws_fact_work_measure (
	date_key string COMMENT '日期代理键',
	product_key string COMMENT '产品代理键',
	sub_company_name string comment '子公司',
	apply_no string COMMENT '订单编号',
	product_version string COMMENT '版本', 
	start_time timestamp COMMENT '环节开始时间',
	end_time timestamp COMMENT '环节完结时间',
	jiedian_name string COMMENT '服务节点',
	sub_department_name string comment '子部门',
	jingban_user string comment '节点经办人'
);

insert overwrite table dws.dws_fact_work_measure
SELECT
cast(dd.id as string) date_key
,cast(dp.id as string) product_key
,a.branch_name sub_company_name
,a.apply_no
,a.product_version
,a.start_time
,a.end_time
,a.jiedian_name
,a.department_name sub_department_name
,a.jingban_user
FROM dws.tmp_fact_work_measure a
left join dws.dimension_date dd on to_date(a.apply_time) = dd.calendar
left join dws.dimension_product dp on a.product_name = dp.product_name
