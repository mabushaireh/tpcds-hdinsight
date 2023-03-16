
USE ${hivevar:ORCDBNAME};

set hive.query.name=${hivevar:QUERY}_call_center;
analyze table call_center compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_catalog_page;
analyze table catalog_page compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_catalog_sales;
analyze table catalog_sales compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_customer;
analyze table customer compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_customer_address;
analyze table customer_address compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_customer_demographics;
analyze table customer_demographics compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_date_dim;
analyze table date_dim compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_household_demographics;
analyze table household_demographics compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_income_band;
analyze table income_band compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_inventory;
analyze table inventory compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_item;
analyze table item compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_reason;
analyze table reason compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_ship_mode;
analyze table ship_mode compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_store;
analyze table store compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_store_returns;
analyze table store_returns compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_store_sales;
analyze table store_sales compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_time_dim;
analyze table time_dim compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_warehouse;
analyze table warehouse compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_web_page;
analyze table web_page compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_web_returns;
analyze table web_returns compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_web_sales;
analyze table web_sales compute statistics for columns;

set hive.query.name=${hivevar:QUERY}_web_site;
analyze table web_site compute statistics for columns;