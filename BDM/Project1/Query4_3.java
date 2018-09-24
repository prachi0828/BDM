customerTable = LOAD '/user/hadoop/input/customers.csv' USING PigStorage(',') AS (customerId,name,age,countryCode,salary);

GroupByCountryCode = GROUP customerTable BY countryCode;

CountByCountryCode = FOREACH GroupByCountryCode GENERATE $0,COUNT($1);

CountByCountryCodeFilter= FILTER CountByCountryCode BY $1>5000 OR $1<2000;

STORE CountByCountryCodeFilter INTO 'pig_output_Query4_3' USING PigStorage('\t');
