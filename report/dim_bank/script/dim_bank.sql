drop table if exists dws.dimtmp_bank;
create table if not exists dws.dimtmp_bank (
  bank_name_short string comment '银行简称'
  ,bank_name string comment '银行名称'
  ,s_key BIGINT comment '代理键'
  ) ;

insert overwrite table dws.dimtmp_bank
select 
case 
when locate("中国银行",a.bank_name)>0 or locate("中国人民银行",a.bank_name)>0 then "中国银行"
when locate("工行",a.bank_name)>0 then "工商银行"
when locate("招行",a.bank_name)>0 then "招商银行"
when locate("浦东发展银行",a.bank_name)>0 then "浦发银行"
when locate("邮政储蓄银行",a.bank_name)>0 or locate("邮政银行",a.bank_name)>0 then "邮储银行"
when  a.bank_name in ("中原","光大","招商","郑州") then concat(a.bank_name ,"银行")
when locate("农村商业",a.bank_name)>0 then
concat(substr(a.bank_name ,1,locate("农村商业",a.bank_name)-1),"农商行")
when locate("农村信用",a.bank_name)>0 then
concat(substr(a.bank_name ,1,locate("农村信用",a.bank_name)-1),"农信社")
when locate("银行",a.bank_name)>0 then 
REPLACE(substr(a.bank_name ,1,locate("银行",a.bank_name)+5),"中国","")
when a.bank_name ="0" then '无'
when a.bank_name ="" then '无'
when a.bank_name is null then '无'
else bank_name
end bank_name_short -- 银行简称
,a.bank_name -- 银行
,row_number() over(order by a.bank_name) s_key
from( 
  select distinct bank_name 
  from(

    select partner_bank_name bank_name
    from ods.ods_bpms_biz_apply_order_common
    group by partner_bank_name

    UNION

    select new_loan_bank_name bank_name
    from ods.ods_bpms_biz_new_loan_common 
    where rn=1
    group by new_loan_bank_name

    union 

    select ori_loan_bank_name bank_name
    from ods.ods_bpms_biz_ori_loan_common 
    where rn=1
    group by ori_loan_bank_name


  )aa
  where bank_name is not null
)a
;

drop table if exists dws.dim_bank;
ALTER TABLE dws.dimtmp_bank RENAME TO dws.dim_bank;


