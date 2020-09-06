use ods;
drop table if exists dwd.dwdtmp_pad_order;
create table dwd.dwdtmp_pad_order(
apply_no STRING COMMENT '订单编号',
branch_name STRING COMMENT '分公司',
house_area STRING COMMENT '所在区域',
product_name STRING COMMENT '产品名称',
apply_status_name STRING COMMENT '订单状态',
prefile_time TIMESTAMP COMMENT '归档时间',
rob_user_name STRING COMMENT '运营跟单人',
operate_type STRING COMMENT '操作类型',
matter_name STRING COMMENT '事项名称',
operate_user_name STRING COMMENT '事项操作人',
operate_time TIMESTAMP COMMENT '事项时间',
target_user_name STRING COMMENT '事项目标人'
)  STORED AS parquet;
insert overwrite table dwd.`dwdtmp_pad_order`
select
bao.apply_no
,dca.company_name_2_level city_name
,bh.house_area
,bao.product_name
,bao.apply_status_name
,t666.prefile_time
,bao.rob_user_name
,CASE
    t2.`operate_type`
    WHEN 'ROB'
    THEN '抢单'
    WHEN 'ASSIGN'
    THEN '派单'
    WHEN 'TRANSFER'
    THEN '转单'
    WHEN 'TRANSFER_PC'
    THEN 'PC派单'
    WHEN 'TRANSFER_ORDER_POOL_IN'
    THEN '转单'
    WHEN 'RELEASE'
    THEN '释放'
    WHEN 'RELEASE_RECEIVE'
    THEN '释放指定人员'
    ELSE t2.`operate_type`
  END operate_type
,CASE
    t2.`operate_type`
    WHEN 'ROB'
    THEN  t3.`matter_name`
    ELSE t2.`matter_name`
  END matter_name
,t2.operate_user_name operate_user_name
,t2.create_time operate_time
,case when t2.`operate_type` = 'TRANSFER_ORDER_POOL_IN' then t21.operate_user_name
else t2.`target_user_name` end target_user_name
from (select * from ods_bpms_biz_handle_order_log_common where operate_type not in ('TRANSFER_ORDER_POOL_LOCK') )t2
left join (select apply_no,rn,operate_user_name,target_user_name from ods_bpms_biz_handle_order_log_common where operate_type in ('TRANSFER_ORDER_POOL_LOCK')) t21 on t21.apply_no=t2.apply_no and t2.operate_type='TRANSFER_ORDER_POOL_IN' and t21.rn=t2.rn
left join ods_bpms_biz_apply_order_common bao on bao.apply_no=t2.apply_no
left join ods_bpms_bpm_check_opinion_common_ex_wx t666 on t666.apply_no=bao.apply_no
   left join dim.dim_company dca on bao.branch_id = dca.company_id_3_level
   left join (select * from ods_bpms_biz_house_common where rn=1) bh on bh.apply_no=bao.apply_no
left join (select apply_no,matter_name from ods_bpms_biz_appoint_info_common  where is_nearly ='1' and rn=1 )t3 on t3.apply_no=t2.apply_no;
drop table if exists dwd.dwd_pad_order;
ALTER TABLE dwd.dwdtmp_pad_order RENAME TO dwd.dwd_pad_order;