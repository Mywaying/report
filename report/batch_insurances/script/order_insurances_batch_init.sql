use ods;
truncate table `dwd`.dwd_order_insurances;
-- 更新信息　
insert overwrite table `dwd`.dwd_order_insurances
SELECT
a.id, -- as '主键id',
a.apply_no, -- as '订单编号',
a.insurance_no, -- as '保单号',
a.insurance_type, -- as '保险类型',
a.insurance_name, -- as '保险名称',
a.insurer_name, -- as '投保人姓名',
a.insurer_cert_id, -- as '投保人证件号码',
a.insurer_phone, -- as '投保人联系电话',
a.insurer_mail, -- as '投保人电子邮件',
a.insurer_adress, -- as '投保人联系地址',
a.insured_name, -- as '被保险人姓名',
a.insured_cret_id, -- as '被保险人证件号码',
a.insured_phone, -- as '被保险人联系电话',
a.insured_mail, -- as '被保险人电子邮件',
a.premium_amount, -- as '保费',
a.insurance_amount, -- as '保险金额',
a.payment_way, -- as '收费方式',
a.payment_party, -- as '保费缴纳方',
a.insurance_period, -- as '保险期限/履约期',
a.preliminary_pass_time, -- as '预审通过时间',
a.receive_policy_time, -- as '接收报单时间',
a.underwrite_pass_time, -- as '核保通过时间',
a.premium_payment_time, -- as '保费缴纳时间',
a.remark, -- as '保险备注',
a.create_user_id, -- as '创建人id',
a.update_user_id, -- as '更新人id',
a.create_time, -- as '记录创建时间',
a.update_time, -- as '记录更新时间',
a.rev, -- as '版本号',
a.delete_flag, -- as '记录删除标识(1：删除；0：有效记录)',
a.is_pass, -- as '是否核保通过',
a.robot_status -- as '机器人操作状态(success,failure)'
from ods_bpms_biz_insurance_info  a