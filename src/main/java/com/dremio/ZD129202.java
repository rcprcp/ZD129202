/*
 * Copyright (C) 2023 Dremio Corporation
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
package com.dremio;


import java.sql.Array;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ZD129202 {
  private static final Logger LOG = LogManager.getLogger(ZD129202.class);

  public static void main(String... args) {

    ZD129202 zd129202 = new ZD129202();
    zd129202.run();
  }

  void run() {
    // final String arrowFlightURL =
    //   "jdbc:arrow-flight-sql://autorelease:32010/?useEncryption=false&threadPoolSize=15";
    //    final String arrowFlightURL =
    //        "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false;&threadPoolSize=4;";

    // print the available JDBC Drivers.
    LOG.info("Registered JDBC Drivers:");
    try {
      Collections.list(DriverManager.getDrivers())
          .forEach(
              driver ->
                  LOG.info(
                      "Driver class {} (version {}.{})",
                      driver.getClass().getName(),
                      driver.getMajorVersion(),
                      driver.getMinorVersion()));
    } catch (Exception ex) {
      LOG.error("Exception: {}", ex.getMessage(), ex);
      System.exit(4);
    }

    // basic:
    String url = buildAFURL();
    //    String url = buildLegacyURL();
    //    String url = buildPostgresqlURL();
    //    String url = buildTestingURL();
    System.out.println("URL: " + url);

    // will ths work with Properties?
    try (Connection conn = DriverManager.getConnection(url, System.getenv("DREMIO_USER"),
                                                       System.getenv("DREMIO_PASSWORD"))) {

        String sequel = "SELECT * from \"bob_s3\".array_column";
        Statement st = conn.createStatement();

        ResultSet rs = st.executeQuery(sequel);
        while (rs.next()) {
          String s = rs.getString(1);
          Array longArray = rs.getArray(2);
          System.out.println("index " + s + " " + longArray);
          /* int i = 0;
          Object element;
          while ((element = longArray.getArray(i, 1)) != null) {
            System.out.println("element " + element);
          }  */
        }

        rs = st.executeQuery(sequel);
        while (rs.next()) {
          String s = rs.getString("xyz");
          Object o = rs.getObject("arr");
          System.out.println("name " + s + " " + o);
        }
    } catch (SQLException ex) {
      LOG.error("SQLException on ResultSet: {}", ex.getMessage(), ex);
    }
  }

  String buildAFURL() {
    String URL = "jdbc:arrow-flight-sql://localhost:32010/?useEncryption=false";

    return URL;
  }
}