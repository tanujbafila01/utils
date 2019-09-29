select a.cc COUNTRY_OF_PLANT, a.sc STORE_CATEGORY, a.uty THIS_YEAR_UNITS, 
a.uly LAST_YEAR_UNITS, a.dty SALES_THIS_YEAR, a.dly SALES_LAST_YEAR, b.rfx RFX_XVAT, c.dcty DOOR_COUNT_THIS_YEAR, 
c.dcly DOOR_COUNT_LAST_YEAR, (cast(a.uty as float64)-cast(a.uly as float64))*100/cast(a.uly as float64) LY_PERCENTAGE_UNITS,
(cast(a.dty as float64)-cast(a.dly as float64))*100/cast(a.dly as float64) LY_PERCENTAGE_SALES
from 
(SELECT Country_of_plant cc, Store_Category sc, sum(cast(U_SLS_WTD as FLOAT64)) uty, 
sum(cast(U_SLS_WTD_LY as FLOAT64)) uly, sum(cast(R_SLS_XVAT_WTD as FLOAT64)) dty, 
sum(cast(R_SLS_XVAT_WTD_LY as FLOAT64)) dly
FROM `case-study-airflow.open_available_data.by_country` 
where 
Country_of_plant<>'Overall Result' and
Store_Type<>'Result' and
Store_Category<>'Result' and
Store_Type<>'CCV'
group by cc, sc
union all 
select Country_of_plant cc, 'CCV' as sc, sum(cast(U_SLS_WTD as FLOAT64)) uty, 
sum(cast(U_SLS_WTD_LY as FLOAT64)) uly, sum(cast(R_SLS_XVAT_WTD as FLOAT64)) dty, 
sum(cast(R_SLS_XVAT_WTD_LY as FLOAT64)) dly
FROM `case-study-airflow.open_available_data.by_country` 
where 
Country_of_plant<>'Overall Result' and
Store_Type<>'Result' and
Store_Category<>'Result' and
Store_Type='CCV'
group by cc
) a
left join
-- get rf3 xvat
(select Country_of_plant cc, Store_Type st, sum(cast(PLAN_SLS_XVAT as float64)) rfx
from `case-study-airflow.open_available_data.rf` 
where cast(WTD as string)!='WTD'
group by cc, st) b
on a.cc=b.cc and
a.sc=b.st

left join
-- calculate ly and ty
(select Country_of_plant cc, Store_Category sc, count(R_SLS_XVAT_WTD) dcty, count(R_SLS_XVAT_WTD_LY) dcly
from `case-study-airflow.open_available_data.by_store`
where  
Country_of_plant<>'Overall Result' and
Store_Type<>'Result' and
Store_Category<>'Result' and
Store_Type<>'CCV'
group by cc, sc
union all 
select Country_of_plant cc, 'CCV' as sc, count(R_SLS_XVAT_WTD) dcty, count(R_SLS_XVAT_WTD_LY) dcly
from `case-study-airflow.open_available_data.by_store`
where  
Country_of_plant<>'Overall Result' and
Store_Type<>'Result' and
Store_Category<>'Result' and
Store_Type='CCV'
group by cc) c
on a.cc=c.cc and
a.sc=c.sc