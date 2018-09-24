customerTable = LOAD '/user/hadoop/input/customers.csv' USING PigStorage(',') AS (customerId,name,age,countryCode,salary);

transactionTable = LOAD '/user/hadoop/input/transactions.csv' USING PigStorage(',') AS (transactionId,customerId2,transTotal,transNumofItems,transDesc);


clean1 = FOREACH transactionTable GENERATE customerId2, transTotal;

GroupByCustomerId = GROUP clean1 BY $0;


CountByCustomerId = FOREACH GroupByCustomerId GENERATE $0,COUNT($1);

FilterLeastTransaction = FILTER CountByCustomerId BY $1<100;


clean2 = FOREACH customerTable GENERATE customerId, name;


joindata= JOIN FilterLeastTransaction by $0, clean2 by $0;


finaldata = FOREACH joindata GENERATE $1, $3;

STORE finaldata INTO 'pigoutput_Query4_1' USING PigStorage(',');