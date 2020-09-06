set hive.execution.engine=spark;
insert overwrite table dws.tmp_report_revenue_part_two_x
select * from dws.tmp_report_revenue_part_two_x
where etl_update_time <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')

union all

select * from dws.tmp_report_revenue_part_two_add_x