insert overwrite table dws.tmp_report_revenue_part_two_nocity
select * from dws.tmp_report_revenue_part_two_nocity
where etl_update_time <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')

union all

select * from dws.tmp_report_revenue_part_two_add_nocity