# AFJDBC - Arrow Flight JDBC Test 

This program is a very simple test for the Arrow JDBC driver. Remember to update the pom.xml for the particular version
of the arrow flight or Legacy Dremio driver that you want. The program currently has some code to test with Postgres, it'll be pulled out later. 

To build, please ensure you have j17. In one place in the program we use a multi-line string. 

Build via the usual:
```Shell
git clone https://github.com/rcprcp/AFJDBC.git
cd AFJDBC
mvn clean package
```
It will build a runnable jar file. 

There are a number of options which are in various combinations.
 --host localhost --username dremio --port 32010 --password dremio123 --context bob_s3.taxi

Then run it: 
```Shell
export JDK_JAVA_OPTIONS=--add-opens=java.base/java.util=ALL-UNNAMED
java -jar target/AFJDBC-1.0-SNAPSHOT-jar-with-dependencies.jar --host localhost --username dremio --port 32010 --password dremio123 --context bob_s3.taxi
```