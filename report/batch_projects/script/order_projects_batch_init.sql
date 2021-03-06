use ods;
-- 更新信息
insert overwrite table `dwd`.dwd_order_projects SELECT a.id, -- as '主键id',
a.apply_no, -- as '业务申请编号',
a.account_type, -- as '资料托管类型  身份证托管：SFZ 银行卡托管：YHK  房地产证：FDCZ  其他要件托管：QT',
a.account_name, -- as '类型名',
a.account_no, -- as '类型号',
a.card_type, -- as '托管银行卡类型供楼卡:GLK  按揭回款卡:AJHKK 首期款回款卡:SQKHKK',
a.bank_code, -- as '银行编码',
a.bank_name, -- as '银行名称',
a.reback_date, -- as '托管证件归还日期',
a.remark, -- as '备注',
a.create_user_id, -- as '创建人id',
a.update_user_id, -- as '更新人id',
a.create_time, -- as '记录创建时间',
a.update_time, -- as '记录更新时间',
a.is_trust_flag, -- as '是否托管',
a.is_close_flag, -- as '是否关闭网银',
a.storage_time, -- as '入库时间',
a.storage_operator, -- as '入库交接人',
a.storage_operator_id, -- as '入库交接人id',
a.storage_receiver, -- as '入库签收人',
a.storage_receiver_id, -- as '入库签收人id',
a.storageout_time, -- as '出库时间',
a.storageout_operator, -- as '出库交接人',
a.storageout_operator_id, -- as '出库交接人id',
a.storageout_receiver, -- as '出库签收人',
a.storageout_receiver_id, -- as '出库签收人id',
a.usb_key, -- as 'U盾，Y 有，N 无',
b.account_id, -- as '托管资料主键',
b.test_time, -- as '测试时间',
b.test_channel, -- as '测试途径（POS:POS机、DSK:柜台、ATM:自助柜员机、OTE:其它)',
b.test_result, -- as '测试结果（Y正常；N异常）',
b.handle_user_id, -- as '办理人员id',
b.handle_user_name, -- as '办理人员姓名',
b.remark, -- as '备注',
b.create_user_id, -- as '创建人id',
b.update_user_id, -- as '更新人id',
b.create_time, -- as '记录创建时间',
b.update_time -- as '记录更新时间'
from ods_bpms_biz_test_record  b LEFT JOIN ods_bpms_biz_project_account a on b.account_id=a.id
-- WHERE a.apply_no = 'XMC0220190129005'