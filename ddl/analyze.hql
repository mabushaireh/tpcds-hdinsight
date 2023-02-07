set hive.query.name=${hiveconf:QUERY};

USE ${hiveconf:ORCDBNAME};

analyze table call_center compute statistics for columns;
analyze table catalog_page compute statistics for columns;
analyze table catalog_returns compute statistics for columns;
analyze table catalog_sales compute statistics for columns;
analyze table customer compute statistics for columns;
analyze table customer_address compute statistics for columns;
analyze table customer_demographics compute statistics for columns;
analyze table date_dim compute statistics for columns;
analyze table household_demographics compute statistics for columns;
analyze table income_band compute statistics for columns;
analyze table inventory compute statistics for columns;
analyze table item compute statistics for columns;
analyze table promotion compute statistics for columns;
analyze table reason compute statistics for columns;
analyze table ship_mode compute statistics for columns;
analyze table store compute statistics for columns;
analyze table store_returns compute statistics for columns;
analyze table store_sales compute statistics for columns;
analyze table time_dim compute statistics for columns;
analyze table warehouse compute statistics for columns;
analyze table web_page compute statistics for columns;
analyze table web_returns compute statistics for columns;
analyze table web_sales compute statistics for columns;
analyze table web_site compute statistics for columns;
