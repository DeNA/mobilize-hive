select act_date,product, sum(value) as sum from mobilize.hive_test_1 group by act_date,product;
