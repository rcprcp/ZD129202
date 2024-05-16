# AFJDBC - Arrow Flight JDBC Test 

This program is a very simple test for the Arrow JDBC driver. Remember to update the pom.xml for the particular version
of the arrow flight driver that you want.

## Instructions 
Set up an S3 source in Dremio ensure it's set up for CTAS as Iceberg. 

Use the following SQL to create a test table and some test data: 
```sql
create table "bob_s3".array_column (xyz BIGINT, arr LIST(BIGINT));
INSERT into "bob_s3".array_column (xyz, arr) VALUES (1, array[9,8,7]);
INSERT into "bob_s3".array_column (xyz, arr) VALUES (2, array[10,11,12]);
select * from "bob_s3".array_column;
-- drop table bob_s3.array_column;
```
The pom.xml specifies Java 17. 

## Build:
Use the usual:
```Shell
git clone https://github.com/rcprcp/ZD129202.git
cd ZD129202
mvn clean package
```
It will build a runnable jar file. 

There are no command line options.  The program expects environment variables for DREMIO_USER and DREMIO_PASSWORD

Then run it: 
```Shell
export JDK_JAVA_OPTIONS=--add-opens=java.base/java.util=ALL-UNNAMED
java -jar target/ZD129202-1.0-SNAPSHOT-jar-with-dependencies.jar
```