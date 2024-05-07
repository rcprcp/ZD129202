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
package com.dremio.afjdbc;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AFJDBC {
  private static final Logger LOG = LogManager.getLogger(com.dremio.afjdbc.AFJDBC.class);

  @Parameter(
      names = {"--host"},
      description = "Host name for connection",
      required = true)
  private static String host = "";

  @Parameter(
      names = {"-u", "--username"},
      description = "username for connection",
      required = true)
  private static String username = "";

  @Parameter(
      names = {"--password"},
      description = "password for connection",
      required = true)
  private static String password = "";

  @Parameter(
      names = {"--context"},
      description = "Dremio context and table name",
      required = true)
  private String context = "";

  @Parameter(
      names = {"--schema"},
      description = "Postgres context and table name",
      required = false)
  private String schema = "";

  @Parameter(
      names = {"-p", "--port"},
      description = "port number for connection")
  private Integer port = 32010;

  @Parameter(
      names = {"-e", "--use-encryption"},
      description = "Enable ssl (Boolean)")
  private Boolean useEncryption = false;

  // no hostverification
  @Parameter(
      names = {"--disable-certificate-verification"},
      description = "disable Certificate Verification")
  private String disableCertificateVerification = "";

  // non default truststore name
  @Parameter(
      names = {"--trust-store-type"},
      description = "trust store type - JKS, PKCS12")
  private String trustStoreType = "";

  @Parameter(
      names = {"--trust-store-path"},
      description = "path to trust store")
  private String trustStorePath = "";

  // truststore password
  @Parameter(
      names = {"--trust-store-password"},
      description = "trust store password")
  private String trustStorePassword = "";

  @Parameter(
      names = {"-h", "--help"},
      description = "display help output",
      help = true)
  private Boolean help = false;

  @Parameter(
      names = {"-z", "--update"},
      description = "attempt to update")
  private Boolean update = false;

  @Parameter(
      names = {"-d", "--debug"},
      description = "display help output")
  private Boolean debug = false;

  public static void main(String... args) {

    AFJDBC afjdbc = new com.dremio.afjdbc.AFJDBC();

    // parse starting args.
    JCommander jc = JCommander.newBuilder().addObject(afjdbc).build();
    jc.parse(args);

    // verify starting args.
    if (StringUtils.isEmpty(host)
        || StringUtils.isEmpty(username)
        || StringUtils.isEmpty(password)) {
      jc.usage();
      System.out.println("host, username, password cannot be empty");
      System.exit(2);
    }

    afjdbc.run(afjdbc, args);
  }

  void run(AFJDBC afjdbc, String... args) {
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

    String arrowFlightURL;
    // basic:
        String url = buildAFURL();
    //    String url = buildLegacyURL();
//    String url = buildPostgresqlURL();
    System.out.println("URL: " + url);

    // will ths work with Properties?
    try (Connection conn = DriverManager.getConnection(url, username, password)) {

      final String sql = String.format("SELECT * FROM %s", context);
      System.out.println(sql);
      long recordCount = 0;
      Statement stmt = conn.createStatement();

            String str =
                    """
            UPDATE bob_s3.taxi
            SET pickup_datetime = '2013-05-01 00:00:42.111'
            WHERE pickup_datetime = '2013-05-01 00:00:42.444'
            """;

//      String str = String.format("UPDATE %s.tab SET userid = '9' where userid = '4'", schema);


      Statement ps = conn.createStatement();
      int rcode = ps.executeUpdate(str);
      int rowCount = ps.getUpdateCount();

      try (ResultSet rs = stmt.executeQuery(sql)) {
        long start = System.currentTimeMillis();
        while (rs.next()) {
          recordCount++;
          if (recordCount % 1_000_000 == 0) {
            LOG.info("{} records", recordCount);
          }
        }
        long elapsed = System.currentTimeMillis() - start;
        long rps = recordCount / (elapsed / 1000);

        String ans =
            String.format("read %d records in %dms = %s per sec", recordCount, elapsed, rps);
        System.out.println(ans);

      } catch (SQLException ex) {
        LOG.error("SQLException on ResultSet: {}", ex.getMessage(), ex);
        System.exit(5);
      }
    } catch (SQLException ex) {
      System.out.println("URL: " + url);
      System.out.println("SQLException on Connection: " + ex.getMessage());
      ex.printStackTrace();
      System.exit(3);
    }
  }

  String buildAFURL() {
    String URL =
        String.format("jdbc:arrow-flight-sql://%s:%d/?useEncryption=%s", host, port, useEncryption);

    return URL;
  }

  String buildLegacyURL() {
    String URL = String.format("jdbc:dremio:direct=%s:%d", host, port);

    return URL;
  }

  String buildPostgresqlURL() {
    String URL = String.format("jdbc:postgresql://%s:%d/", host, port);

    return URL;
  }
}
