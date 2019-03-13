
  DROP TABLE IF EXISTS schema1.table1;
  CREATE TABLE IF NOT EXISTS schema1.table1(
  key String,
  field1 Integer,
  field2 String,
  field3 String,
  field4 String,
  field5 String)
  USING com.databricks.spark.csv
  OPTIONS (path "${variableDefinedInTheClusterWithThePathForThisEnvironment}schema1/table1",header "true", delimiter ",");



DROP TABLE IF EXISTS schema1.table2;
  CREATE TABLE IF NOT EXISTS schema1.table2(
  key String,
  field1 Integer,
  field2 String,
  field3 String,
  field4 String,
  field5 String)
  USING com.databricks.spark.csv
  OPTIONS (path "${variableDefinedInTheClusterWithThePathForThisEnvironment}schema1/table2",header "true", delimiter ",");


