USE ${hivevar:ORCDBNAME};
set hive.query.name=${hivevar:QUERY};

select i_brand_id brand_id
    , i_brand brand
    , sum(ss_ext_sales_price) ext_price
from date_dim, store_sales, item
where date_dim.d_date_sk = store_sales.ss_sold_date_sk
     and store_sales.ss_item_sk = item.i_item_sk
     and i_manager_id=36
     and d_moy=12
     and d_year=2001
group by i_brand, i_brand_id
order by ext_price desc, i_brand_id
limit 100 
;