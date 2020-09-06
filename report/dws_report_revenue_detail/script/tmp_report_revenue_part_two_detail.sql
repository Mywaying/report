insert overwrite table dws.tmp_report_revenue_part_two_detail
select * from dws.tmp_report_revenue_part_two_detail
where etl_update_time <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')

union all

select * from dws.tmp_report_revenue_part_two_detail_add