
drop table if exists dws.dws_sys_user_snap;
create table if not exists dws.dws_sys_user_snap (   
   s_key bigint comment '代理键',
   user_id STRING COMMENT '用户id',
   user_name STRING COMMENT '用户姓名',
   user_status bigint comment '用户状态： 1 正常， 0 禁用',
   job_code STRING COMMENT '岗位编码',
   job_name STRING COMMENT '岗位',
   sub_department_id STRING comment '子部门id',
   sub_department_name STRING COMMENT '子部门名称',
   org_type STRING COMMENT '组织类型',
   zhuguan_id STRING COMMENT '子部门主管id',
   zhuguan_name STRING COMMENT '子部门主管姓名',
   department_id STRING COMMENT '部门id',
   department_name STRING COMMENT '部门名称',
   sub_company_id STRING COMMENT '分公司id',
   sub_company_name STRING COMMENT '分公司名称',
   company_name STRING COMMENT '公司名称',
   company_type STRING COMMENT '公司类型',
   date_from TIMESTAMP,
   date_to TIMESTAMP ,
   etl_update_time TIMESTAMP COMMENT '更新时间' ) ;
   
   
insert overwrite table dws.dws_sys_user_snap
select row_number() over (order by user_id, user_status, job_code, a.sub_department_id, a.department_id) id, a.*
from dws.dws_sys_user_snap_tmp a

