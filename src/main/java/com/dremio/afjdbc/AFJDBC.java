/*
 * Copyright (C) 2022 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.afjdbc;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Collections;
import java.util.Scanner;

public class AFJDBC {
  private static final Logger LOG = LogManager.getLogger(com.dremio.afjdbc.AFJDBC.class);

  public static void main(String... args) {

    AFJDBC afjdbc = new com.dremio.afjdbc.AFJDBC();
    afjdbc.run();
  }

  void run() {

//    final String arrowFlightURL = "jdbc:arrow-flight-sql://172.25.2.207:32010/?useEncryption=false&threadPoolSize=15";
//    final String arrowFlightURL = "jdbc:arrow-flight-sql://172.25.0.98:32010/?useEncryption=false&threadPoolSize=5";
//    final String arrowFlightURL = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false;&threadPoolSize=4;";
    final String arrowFlightURL = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false;&threadPoolSize=4;";

    // print the available JDBC Drivers.
    LOG.info("Registered JDBC Drivers:");
    try {
      Collections.list(DriverManager.getDrivers()).forEach(driver -> LOG.info("Driver class {} (version {}.{})",
                                                                              driver.getClass().getName(),
                                                                              driver.getMajorVersion(),
                                                                              driver.getMinorVersion()));
    } catch (Exception ex) {
      LOG.error("Exception: {}", ex.getMessage(), ex);
      System.exit(4);
    }

    // Hit return to continue
    Scanner sc = new Scanner(System.in).useDelimiter("[\r\n]");
    System.out.println("Hook up the profiler; start JFR, then hit return");
    sc.nextLine();

    try (Connection conn = DriverManager.getConnection(arrowFlightURL,
                                                       System.getenv("DREMIO_USER"),
                                                       System.getenv("DREMIO_PASSWORD"))) {

      Statement stmt = conn.createStatement();

//      String dataSource = "Samples.\"samples.dremio.com\".\"NYC-taxi-trips\"";
//      String dataSource = "Samples.\"samples.dremio.com\".\"tpcds_sf1000\".\"catalog_sales\"";
        String dataSource = "\"rajan-sample-databucket\".\"case125370_156Cols_20M\"";

      final String sql = String.format("SELECT * FROM %s", dataSource);

      // TODO: do we need this additioal information?  Does it help with testing?
      //  String cat = conn.getCatalog();
      //  DatabaseMetaData dbmd = conn.getMetaData();

      int recordCount = 0;
      long startTime = System.currentTimeMillis();

      try (ResultSet rs = stmt.executeQuery(sql)) {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (rs.next()) {
          // although it's not much processing, let's touch all the columns.
          for (int i = 1; i <= columnCount; i++) {
            switch (metaData.getColumnType(i)) {
              case Types.DOUBLE:
              case Types.REAL:
                Double d = rs.getDouble(i);
                continue;

              case Types.BIGINT:
              case Types.INTEGER:
                Long ll = rs.getLong(1);
                continue;

              case Types.DECIMAL:
                BigDecimal bd = rs.getBigDecimal(i);
                continue;

              case Types.NVARCHAR:
              case Types.CHAR:
              case Types.VARCHAR:
                String str = rs.getString(i);
                continue;

              case Types.DATE:
                java.sql.Date dt = rs.getDate(i);
                continue;

              case Types.TIME:
                java.sql.Time t = rs.getTime(i);
                continue;

              case Types.TIMESTAMP:
                java.sql.Timestamp ts = rs.getTimestamp(i);
                continue;

              default:
                String s = rs.getString(i);
                //TODO:  This may print a lot, should we limit it?
                System.out.println("unhandled type: " + metaData.getColumnType(i));
            }
          }

          recordCount++;
          if (recordCount % 30000 == 0) {
            LOG.info("{} records", recordCount);
          }
        }
        rs.close();
        stmt.close();

      } catch (SQLException ex) {
        LOG.error("SQLException: {}", ex.getMessage(), ex);
        System.exit(5);
      }

      long elapsed = (System.currentTimeMillis() - startTime)/1000;
      long rps = recordCount / elapsed;
      LOG.info("read {} records in {} seconds. {} records per second", recordCount, elapsed, rps);

    } catch (SQLException ex) {
      LOG.error("SQLException: {}", ex.getMessage(), ex);
      System.exit(5);
    }
  }

  void napTime(long ms) {
    try {
      Thread.sleep(ms);
    } catch (InterruptedException ex) {
      System.out.println("InterruptedException: " + ex.getMessage());
      ex.printStackTrace();
      System.exit(3);
    }
  }
}
