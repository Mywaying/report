use ods;
drop table if exists dwd.dwdtmp_orders_update;
CREATE TABLE if not exists dwd.dwdtmp_orders_update (
branch_name STRING comment '子公司',
isjms STRING comment '是否加盟',
product_name STRING comment '产品名称',
apply_no STRING comment '订单编号',
seller_name STRING comment '卖方/产权人姓名',
buy_name STRING comment '买方姓名',
financialarchiveevent_time timestamp comment '财务归档时间',
remind_time STRING comment '提请时点',
datau_time timestamp comment '数据更新时间',
update_module STRING comment '信息模块',
update_module_name STRING comment '信息模块名',
field STRING comment '字段名',
field_name STRING comment '字段名称',
n_value STRING comment '新值',
o_value STRING comment '原值',
remark STRING comment '变更原因',
flow_instance_id STRING comment '流程实例ID',
apply_status_name STRING comment '流程状态',
create_user_id STRING comment '提交人id',
create_user_name STRING comment '提交人',
create_time timestamp comment '提交时间',
operation_supervisor_user_name STRING comment '运营主管审批人',
operation_supervisor_time timestamp comment '运营主管审批时间',
operation_supervisor_option STRING comment '运营主管审批意见',
centralized_operation_manager_user_name STRING comment '集中作业主管审批人',
centralized_operation_manager_time timestamp comment '集中作业主管审批时间',
centralized_operation_manager_option STRING comment '集中作业主管审批意见',
marketing_supervisor_user_name STRING comment '市场主管审批人',
marketing_supervisor_time timestamp comment '市场主管审批时间',
marketing_supervisor_option STRING comment '市场主管审批意见',
centralized_operation_user_name STRING comment '集中作业审批人',
centralized_operation_time timestamp comment '集中作业审批时间',
centralized_operation_option STRING comment '集中作业审批意见',
capitalduty_user_name STRING comment '资金岗审批人',
capitalduty_time timestamp comment '资金岗审批时间',
capitalduty_option STRING comment '资金岗审批意见',
riskmanager_user_name STRING comment '风险部审批人',
riskmanager_time timestamp comment '风险部审批时间',
riskmanager_option STRING comment '风险部审批意见',
sales_operation_user_name STRING comment '销管部审批人',
sales_operation_time timestamp comment '销管部审批时间',
sales_operation_option STRING comment '销管部审批意见');
insert overwrite table dwd.dwdtmp_orders_update
SELECT bao.branch_name
,bao.isjms
,bao.product_name
,bau.apply_no
,bao.seller_name_new
,bao.buy_name_new
,t666.financialarchiveevent_time
,case when bdu.create_time >t666.financialarchiveevent_time then "财务归档后" else "归档前" end
,from_timestamp(bau.datau_time,'yyyy-MM-dd HH:mm:ss')
,bau.update_module
,bau.update_module_name
,bau.field
,bau.field_name
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(bau.n_value), 'cqr', '产权人'),'cqrpo','产权人配偶'),'jkr','原贷款借款人')
,'jkrpo','原贷款借款人配偶'),'xdkjkrpo','新贷款借款人配偶')
,regexp_replace(regexp_replace(regexp_replace(regexp_replace(regexp_replace(lower(bau.o_value), 'cqr', '产权人'),'cqrpo','产权人配偶'),'jkr','原贷款借款人')
,'jkrpo','原贷款借款人配偶'),'xdkjkrpo','新贷款借款人配偶')
,case when instr(bau.remark,'其他')>0 then concat(bau.remark,';',bau.other_remark)
else bau.remark end
,bau.flow_instance_id
,bdu.flow_status_name
,bdu.create_user_id
,bdu.create_user_name
,from_timestamp(bdu.create_time,'yyyy-MM-dd HH:mm:ss')
,t888.operation_supervisor_user_name
,t888.operation_supervisor_time
,t888.operation_supervisor_option
,t888.centralized_operation_user_name
,t888.centralized_operation_time
,t888.centralized_operation_option
,t888.marketing_supervisor_user_name
,t888.marketing_supervisor_time
,t888.marketing_supervisor_option
,t888.centralized_operation_manager_user_name
,t888.centralized_operation_manager_time
,t888.centralized_operation_manager_option
,t888.capitalduty_user_name
,t888.capitalduty_time
,t888.capitalduty_option
,t888.riskmanager_user_name
,t888.riskmanager_time
,t888.riskmanager_option
,t888.sales_operation_user_name
,t888.sales_operation_time
,t888.sales_operation_option
from dwd.dwdtmp_update_data_detail bau LEFT JOIN
ods_bpms_biz_apply_order_common bao on bao.apply_no=bau.apply_no
LEFT JOIN ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bau.apply_no
left join ods_bpms_biz_data_update_common bdu on bdu.id=bau.id
left join ods_bpms_bpms_bpm_check_opinion_flow_wx t888 on t888.proc_inst_id_=bau.flow_instance_id;
drop table if exists dwd.dwd_orders_update;
ALTER TABLE dwd.dwdtmp_orders_update RENAME TO dwd.dwd_orders_update;