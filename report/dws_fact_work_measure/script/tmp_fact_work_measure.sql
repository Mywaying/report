set hive.execution.engine=spark;
drop table if exists dws.tmp_fact_work_measure;
create table if not exists dws.tmp_fact_work_measure (
	apply_no string comment "订单号"
	, apply_time timestamp comment "申请时间"
	, product_name string comment "产品名称"
	, product_id string comment "产品id"
	, branch_id string comment "分公司id"
	, product_version string
	, start_time timestamp comment '开始时间'
	, end_time timestamp comment '结束时间'
	, jingban_user string comment '经办人'
	, jingban_user_id string comment '经办人id'
	, jiedian_key string  comment '节点id'
	, jiedian_name string  comment '节点名称'
	, sales_branch_id string  comment '渠道经理部门id'
	, branch_name string comment '分公司名称'
	, department_name string comment '渠道经理部门'
);

with tmp_all_data as (
	select
	bao.apply_no
	,bao.apply_time
	,bao.product_name
	,bao.product_id
	,bao.branch_id
	,bao.product_version
	,bomr.create_time  start_time
	,bomr.handle_time  end_time
	,bomr.handle_user_name jingban_user
	,bomr.handle_user_id  jingban_user_id
	,bomr.matter_key jiedian_key
	,bomr.matter_name  jiedian_name
	,bao.sales_branch_id
	from  ods.ods_bpms_biz_apply_order_common bao
	join ods.ods_bpms_biz_order_matter_record bomr on bao.apply_no = bomr.apply_no
	where bomr.handle_time is not null
	and bomr.matter_name in ("面签" ,"办理公证" ,"申请贷款" ,"预约赎楼" ,"同贷信息登记" ,"同贷登记"
	,"赎楼登记","领取注销资料","注销抵押","注销抵押_郑州","要件托管","账户测试"
	,"过户递件","过户递件_郑州","过户出件","过户出件_郑州","抵押递件","抵押递件_郑州"
	,"抵押出件", "抵押出件_郑州","查档","下户调查","贷款上报终审"
	,"资料齐全","放款登记","房产评估","查房")

	union all

	select
	bao.apply_no
	,bao.apply_time
	,bao.product_name
	,bao.product_id
	,bao.branch_id
	,bao.product_version
	,bof.create_time start_time
	,bof.handle_time end_time
	,bof.handle_user_name jingban_user
	,bof.handle_user_id jingban_user_id
	,bof.flow_type jiedian_key
	,nvl(c.NAME_, d.NAME_) jiedian_name
	,bao.sales_branch_id
	from  ods.ods_bpms_biz_apply_order_common bao
	join ods.ods_bpms_biz_order_flow bof on bao.apply_no = bof.apply_no
	left join ods.ods_bpms_sys_dic c on bof.flow_type = c.KEY_ and c.TYPE_ID_="10000030350009"
	left join ods.ods_bpms_sys_dic d on bof.flow_type = d.KEY_ and d.TYPE_ID_="10000033420071"
	where bof.handle_time is not null
	and nvl(c.NAME_, d.NAME_) in ("面签" ,"办理公证" ,"申请贷款" ,"预约赎楼" ,"同贷信息登记" ,"同贷登记"
	,"赎楼登记","领取注销资料","注销抵押","注销抵押_郑州","要件托管","账户测试"
	,"过户递件","过户递件_郑州","过户出件","过户出件_郑州","抵押递件","抵押递件_郑州"
	,"抵押出件", "抵押出件_郑州","查档","下户调查","贷款上报终审"
	,"资料齐全","放款登记","房产评估","查房")

	union all

	select
	bao.apply_no
	,bao.apply_time
	,bao.product_name
	,bao.product_id
	,bao.branch_id
	,bao.product_version
	,bco.CREATE_TIME_ start_time
	,bco.COMPLETE_TIME_ end_time
	,bco.AUDITOR_NAME_ jingban_user
	,bco.AUDITOR_ jingban_user_id
	,bco.TASK_KEY_ jiedian_key
	,bco.TASK_NAME_ jiedian_name
	,bao.sales_branch_id
	from  ods.ods_bpms_biz_apply_order_common bao
	join ods.ods_bpms_bpm_check_opinion bco on bao.flow_instance_id = bco.PROC_INST_ID_
	where bco.COMPLETE_TIME_ is not null
	and bco.STATUS_ not in ("end", "manual_end")
	and bco.TASK_NAME_ in ("面签" ,"办理公证" ,"申请贷款" ,"预约赎楼" ,"同贷信息登记" ,"同贷登记"
	,"赎楼登记","领取注销资料","注销抵押","注销抵押_郑州","要件托管","账户测试"
	,"过户递件","过户递件_郑州","过户出件","过户出件_郑州","抵押递件","抵押递件_郑州"
	,"抵押出件", "抵押出件_郑州","查档","下户调查","贷款上报终审"
	,"资料齐全","放款登记","房产评估","查房")

	union all

	--  获取节点中的嵌套节点
	select
	bao.apply_no
	,bao.apply_time
	,bao.product_name
	,bao.product_id
	,bao.branch_id
	,bao.product_version
	,bco.CREATE_TIME_ start_time
	,bco.COMPLETE_TIME_ end_time
	,bco.AUDITOR_NAME_ jingban_user
	,bco.AUDITOR_ jingban_user_id
	,bco.TASK_KEY_ jiedian_key
	,bco.TASK_NAME_ jiedian_name
	,bao.sales_branch_id
	from ods.ods_bpms_biz_apply_order_common bao
	join ods.ods_bpms_bpm_check_opinion bco on bao.flow_instance_id = bco.SUP_INST_ID_
	where bco.COMPLETE_TIME_ is not null
	and bco.STATUS_ not in ("end", "manual_end")
	and bco.TASK_NAME_ in ("面签" ,"办理公证" ,"申请贷款" ,"预约赎楼" ,"同贷信息登记" ,"同贷登记"
	,"赎楼登记","领取注销资料","注销抵押","注销抵押_郑州","要件托管","账户测试"
	,"过户递件","过户递件_郑州","过户出件","过户出件_郑州","抵押递件","抵押递件_郑州"
	,"抵押出件", "抵押出件_郑州","查档","下户调查","贷款上报终审"
	,"资料齐全","放款登记","房产评估","查房")

)

insert overwrite table dws.tmp_fact_work_measure
select 
 a.apply_no, a.apply_time, a.product_name, a.product_id, a.branch_id, a.product_version
 ,a.start_time, a.end_time, a.jingban_user, a.jingban_user_id, a.jiedian_key, a.jiedian_name
 ,a.sales_branch_id, a.branch_name, a.department_name
from(
	select
	 a.apply_no, a.apply_time, a.product_name, a.product_id, a.branch_id, a.product_version
	 ,a.start_time, a.end_time, a.jingban_user, a.jingban_user_id, a.jiedian_key, a.jiedian_name
	 ,a.sales_branch_id , b.name_ branch_name, c.name_ department_name
	 ,row_number() over(PARTITION BY a.apply_no, a.jiedian_name, date_format(a.start_time, 'yyyy-MM-dd HH:mm') ORDER BY a.start_time) rn
	from tmp_all_data a
	left join ods.ods_bpms_sys_org b on a.branch_id = b.code_
	left join ods.ods_bpms_sys_org c on a.sales_branch_id = c.code_
) as a
where rn = 1
	

