set hive.execution.engine=spark;
drop table if exists dwd.tmp_report_risk_node_info_p3;
create table if not exists dwd.tmp_report_risk_node_info_p3 (
  `apply_no` string,
  `remark` string,
  `remark_level_1` string COMMENT '一级分类',
  `remark_level_2` string COMMENT '二级分类',
  `remark_level_3` string COMMENT '三级分类'
);

insert overwrite table dwd.tmp_report_risk_node_info_p3
select 
apply_no
, remark
, split(remark, '-')[0]
, split(remark, '-')[1]
, split(remark, '-')[2]
from ods.ods_bpms_biz_missing_materials 
where node_id = "Investigate" and remark is not null and  remark <> '' and material_name = "资料上传意见"