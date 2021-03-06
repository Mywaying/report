use ods;
truncate table `dwd`.dwd_order_credit_risks;
-- 更新信息
insert into `dwd`.dwd_order_credit_risks (id,
apply_no,
customer_no,
customer_name,
credit_type,
parse_time,
parse_ret_time,
credit_channel,
report_no,
report_from,
parse_way,
credit_status,
task_id,
cache_flag,
operate_user_id,
operate_user_name,
manual_credit_query_date,
manual_credit_name,
manual_is_sensitive_career,
manual_cert_type,
manual_cert_no,
manual_query_times_month3,
manual_query_times_month12,
manual_m2_overdue_has,
manual_m2_overdue_name,
manual_m4_overdue_has,
manual_m4_overdue_name,
manual_month12_overdue_time,
manual_month12_overdue_name,
manual_month6_overdue_time,
manual_month6_overdue_name,
manual_dgz_has,
manual_dgz_name,
manual_loan_status_json,
manual_loan_end_date_json,
manual_account_info_json,
manual_guarantee_info_json,
manual_new_loan_has,
manual_new_loan_count,
manual_new_loan_amount,
manual_card_use_amount,
manual_card_credit_line,
manual_overdraft70_percent,
manual_credit_total_amount,
manual_loan_use_limit,
manual_loan_overdue_amount,
manual_remark,
create_user_id,
update_user_id,
create_time,
update_time,
-- credit_num,
credit_modify_time,
credit_operate_user
)SELECT id, -- as '主键id',
apply_no, -- as '订单编号',
customer_no, -- as '客户编号',
customer_name, -- as '客户姓名',
credit_type, -- as '征信类型',
parse_time, -- as '解析时间',
parse_ret_time, -- as '解析完成时间',
credit_channel, -- as '征信获取方式',
report_no, -- as '征信报告编号',
report_from, -- as '征信报告来源',
parse_way, -- as '解析方式',
credit_status, -- as '征信状态',
task_id, -- as '唯一任务标识(半刻查询结果使用)',
cache_flag, -- as '是否走了缓存，未向机构发起解析请求(Y：本次解析走缓存N：本次解析未走缓存)',
operate_user_id, -- as '操作人员id',
operate_user_name, -- as '操作人员姓名',
manual_credit_query_date, -- as '征信查询日期',
manual_credit_name, -- as '姓名',
manual_is_sensitive_career, -- as '是否为敏感职业',
manual_cert_type, -- as '证件类型',
manual_cert_no, -- as '证件号码',
manual_query_times_month3, -- as '近3月查询次数',
manual_query_times_month12, -- as '近12月查询次数',
manual_m2_overdue_has, -- as '近3月有无M2以上逾期',
manual_m2_overdue_name, -- as '近3月M2以上逾期信贷名称',
manual_m4_overdue_has, -- as '近12月有无M4以上逾期',
manual_m4_overdue_name, -- as '近12月M4以上逾期信贷名称',
manual_month12_overdue_time, -- as '近12月逾期次数',
manual_month12_overdue_name, -- as '近12月逾期信贷名称',
manual_month6_overdue_time, -- as '近6月逾期次数',
manual_month6_overdue_name, -- as '近6月逾期信贷名称',
manual_dgz_has, -- as '近24月有无“D/G/Z”',
manual_dgz_name, -- as '近24月“D/G/Z”信贷名称',
manual_loan_status_json, -- as '贷款状态列表JSON：贷款名称，贷款账户状态，五级分类情况',
manual_loan_end_date_json, -- as '贷款到期列表JSON：贷款名称，到期日，本金余额',
manual_account_info_json, -- as '准/贷记卡账户信息列表JSON：名称，账户状态，当前预期次数',
manual_guarantee_info_json, -- as '担保信息列表JSON：编号，本金余额，五级分类',
manual_new_loan_has, -- as '有无新增无抵/质押贷款',
manual_new_loan_count, -- as '新增无抵/质押贷款合计笔数',
manual_new_loan_amount, -- as '新增无抵/质押贷款合计金额',
manual_card_use_amount, -- as '（准）贷记卡使用金额'
manual_card_credit_line, -- as '（准）贷记卡总授信金额',
manual_overdraft70_percent, -- as '是否超过70%;百分比',
manual_credit_total_amount, -- as '无抵/质押贷款余额',
manual_loan_use_limit, -- as '贷记卡使用额度',
manual_loan_overdue_amount, -- as '准贷记卡透支余额',
remark, -- as '备注',
create_user_id, -- as '创建人id',
update_user_id, -- as '更新人id',
create_time, -- as '记录创建时间',
update_time, -- as '记录更新时间',
-- credit_num -- as '上传数量',
credit_modify_time, -- as '最后上传时间',
credit_operate_user -- as '最后上传征信操作人'
from ods_bpms_biz_query_credit;
-- WHERE a.customer_no ='00203449';


-- 更新信息
insert into `dwd`.order_credit_risks (id,
apply_no,
customer_no,
customer_name,
business_work_unit,
business_query_time,
business_is_self_employed,
business_self_employed_type,
business_query_result,
business_query_user_id,
business_query_user_name,
business_business_remark,
business_create_user_id,
business_update_user_id,
business_create_time,
business_update_time,
business_flag,
business_type_json
)SELECT b.id, -- as '主键id',
b.apply_no, -- as '订单编号',
b.customer_no, -- as '客户编号',
b.customer_name, -- as '客户姓名',
b.work_unit, -- as '工作单位',
b.query_time, -- as '查询时间',
b.is_self_employed, -- as '是否为自雇人士',
b.self_employed_type, -- as '自雇人士类型',
b.query_result, -- as '查询结果',
b.query_user_id, -- as '查询人id',
b.query_user_name, -- as '查询姓名',
b.remark, -- as '备注',
b.create_user_id, -- as '创建人id',
b.update_user_id, -- as '更新人id',
b.create_time, -- as '记录创建时间',
b.update_time, -- as '记录更新时间',
b.flag, -- as '请求结果-Y：请求成功；N：请求失败',
b.type_json -- as '会将查询参数保存在这里'
from ods_bpms_biz_business_info  b
-- WHERE b.customer_no ='00203449';