set hive.execution.engine=spark;
insert overwrite table dws.tmp_dws_order_total_agg
select * from dws.tmp_dws_order_total_agg
where to_date(etl_update_time) <> from_unixtime(unix_timestamp(),'yyyy-MM-dd')
union all
select * from dws.tmp_dws_order_total_agg_add;