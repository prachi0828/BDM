customerTable = LOAD '/user/hadoop/input/customers.csv' USING PigStorage(',') AS (customerId,name,age,countryCode,salary);

transactionTable = LOAD '/user/hadoop/input/transactions.csv' USING PigStorage(',') AS (transactionId,customerId2,transTotal:float,transNumofItems,transDesc);


clean1 = FOREACH transactionTable GENERATE customerId2, transTotal, transNumofItems;

GroupByCustomerId = GROUP clean1 BY $0;


ByCustomerId = FOREACH GroupByCustomerId GENERATE $0,COUNT($1),SUM(clean1.transTotal),MIN

clean2 = FOREACH customerTable GENERATE customerId, name, salary;


joindata= JOIN ByCustomerId by $0, clean2 by $0;


finaldata = FOREACH joindata GENERATE $0, $5, $6, $1, $2, $3;
STORE finaldata INTO 'pigoutput_Query4_2' USING PigStorage('\t');