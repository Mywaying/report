use ods;
drop table if exists dwd.dwdtmp_work_order;
create table dwd.dwdtmp_work_order
(
    apply_no                  STRING COMMENT '订单编号',
    material_name             STRING COMMENT '补件清单',
    material_reason           STRING COMMENT '补件原因',
    material_finish_time      TIMESTAMP COMMENT '补件完成时间',
    material_start_time       TIMESTAMP COMMENT '补件发起时间',
    material_last_finish_time TIMESTAMP COMMENT '最终补件完成时间',
    input_info_create_time    TIMESTAMP COMMENT '录入创建时间',
    input_info_lock_time      TIMESTAMP COMMENT '录入锁定时间'
) STORED AS parquet;
with temp_check_opinion AS (
    SELECT PROC_INST_ID_,
           ASSIGN_TIME_,
           CREATE_TIME_,
           row_number()
                   over (PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN ASSIGN_TIME_ IS NULL OR ASSIGN_TIME_ = '' THEN 1 ELSE 0 END ASC,ASSIGN_TIME_ ASC) rank1,
           row_number()
                   over (PARTITION BY PROC_INST_ID_,TASK_KEY_ ORDER BY CASE WHEN CREATE_TIME_ IS NULL OR CREATE_TIME_ = '' THEN 1 ELSE 0 END ASC,CREATE_TIME_ ASC) rank2
    FROM ods_bpms_bpm_check_opinion_common
    where LOWER(TASK_KEY_) = 'inputinfo'
),
     temp_check_opinion_sort AS (
         SELECT PROC_INST_ID_,
                min(CASE
                        WHEN rank1 = 1
                            THEN (CAST(ASSIGN_TIME_ AS STRING)) END) inputinfo_min_at, -- 录入创建时间
                min(CASE
                        WHEN rank2 = 1
                            THEN (CAST(CREATE_TIME_ AS STRING)) END) inputinfo_min_ct  -- 录入锁定时间
         FROM temp_check_opinion
         GROUP BY PROC_INST_ID_
     ),
     temp_missing_materials AS (
         SELECT apply_no,
                material_code,
                material_name, -- 补件清单
                remark,        --补件原因
                create_time,   -- 补件发起时间
                update_time,   --补件完成时间
                delete_flag,   --是否补件标识 (1:完成;0:待补件)
                row_number()
                        over (PARTITION BY apply_no,material_code ORDER BY CASE WHEN create_time IS NULL OR create_time = '' THEN 1 ELSE 0 END ASC,create_time ASC)                                             rank1,
                row_number()
                        over (PARTITION BY apply_no,material_code ORDER BY CASE WHEN create_time IS NULL OR create_time = '' THEN 1 ELSE 0 END ASC,create_time DESC)                                            rank2,
                row_number()
                        over (PARTITION BY apply_no ORDER BY CASE WHEN material_code = '000000' THEN 1 ELSE 0 END ASC,CASE WHEN create_time IS NULL OR create_time = '' THEN 1 ELSE 0 END ASC,create_time DESC) rank3
         FROM ods_bpms_biz_missing_materials
         where LOWER(node_id) = 'inputinfo'
     ),
     temp_missing_materials_sort AS (
         SELECT apply_no,
                min(CASE
                        WHEN rank3 = 1
                            THEN material_name END) material_name,
                min(CASE
                        WHEN rank1 = 1 and material_code = '000000'
                            THEN remark END) material_reason,
                min(CASE
                        WHEN rank1 = 1 and material_code = '000000' and delete_flag = '1'
                            THEN (CAST(update_time AS STRING)) END) material_finish_time,
                min(CASE
                        WHEN rank1 = 1 and material_code = '000000'
                            THEN (CAST(create_time AS STRING)) END) material_start_time,
                min(CASE
                        WHEN rank2 = 1 and material_code = '000000' and delete_flag = '1'
                            THEN (CAST(update_time AS STRING)) END) material_last_finish_time
         FROM temp_missing_materials
         GROUP BY apply_no
     )

insert
overwrite
table
dwd.`dwdtmp_work_order`
select bao.apply_no,
       tmms.material_name,
       tmms.material_reason,
       tmms.material_finish_time,
       tmms.material_start_time,
       tmms.material_last_finish_time,
       tcos.inputinfo_min_ct,
       tcos.inputinfo_min_at
FROM (SELECT * FROM ods_bpms_biz_apply_order_common WHERE apply_no != '') bao
         LEFT JOIN temp_missing_materials_sort tmms ON tmms.apply_no = bao.apply_no
         LEFT JOIN temp_check_opinion_sort tcos ON tcos.PROC_INST_ID_ = bao.flow_instance_id;
drop table if exists dwd.dwd_work_order;
ALTER TABLE dwd.dwdtmp_work_order
    RENAME TO dwd.dwd_work_order;