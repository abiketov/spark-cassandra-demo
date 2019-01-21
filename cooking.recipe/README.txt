Spark version 2.4.0

These jars must be in current folder when job is submited:
. /spark-cassandra-connector_2.11-2.4.0.jar
./config-1.3.3.jar
./jsr166e-1.1.0.jar

DataStax Cassandra database distribution was used running locally
Spark jobs executed with master local

To build project:
mvn package

Create keyspace and tables in Cassandra run script database_init.cql

SPARK_HOME system variable must be set

recipe_events.csv file contains generated records that where used for testing

To run data load:
run-load-job-dataframe.bat OR run-load-job-streamimg.bat 
run-load-job-dataframe.bat - DataFrame version
run-load-job-streamimg.bat - Streaming version

To run analytics job:
run-analytics-job.bat

Testing was done only in Windows environment with Spark and Cassandra installed loccaly.

To pass parameters to data load job run-load-job-dataframe.bat needs to be updated
Example:

%SPARK_HOME%\bin\spark-submit2 --master local --class com.cooking.recipe.DataLoadDataFrame --jars ./spark-cassandra-connector_2.11-2.4.0.jar,./config-1.3.3.jar,./jsr166e-1.1.0.jar ./cooking.job/target/cooking.job-1.0-SNAPSHOT.jar 127.0.0.1 <file_name> <file_path> 
Parameters must be passed in that order, so you can't pass file path but not pass file name.
