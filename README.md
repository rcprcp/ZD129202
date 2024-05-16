# AFJDBC - Arrow Flight JDBC Test 

This program is a very simple test for the Arrow JDBC driver. Remember to update the pom.xml for the particular version
of the arrow flight driver that you want.

The pom.xml specifies Java 17. 

Build via the usual:
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