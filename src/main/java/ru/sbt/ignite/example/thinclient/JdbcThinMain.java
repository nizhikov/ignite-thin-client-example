/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ru.sbt.ignite.example.thinclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import static ru.sbt.ignite.example.thinclient.ThinClientMain.INSERT_QRY;
import static ru.sbt.ignite.example.thinclient.ThinClientMain.SELECT_QRY;

/**
 */
public class JdbcThinMain {
    public static void main(String[] args) throws Exception {
        Class.forName("org.apache.ignite.IgniteJdbcThinDriver");

        try (Connection conn = DriverManager.getConnection("jdbc:ignite:thin://127.0.0.1")) {
            deleteAll(conn);
            
            insertRow(conn, "JDBC", "ADDR", 127000);
            insertRow(conn, "JDBC-2", "ADDR2", 127000);

            try {
                insertRow(conn, "JDBC-2", "Long Address", 127000);

                throw new IllegalStateException("This should fail");
            }
            catch (Exception e) {
                //Ignore.
            }

            printAll(conn);
        }
    }

    private static void printAll(Connection conn) throws SQLException {
        try (PreparedStatement stmnt = conn.prepareStatement(SELECT_QRY);
            ResultSet rs = stmnt.executeQuery()) {
            while (rs.next()) {
                System.out.format("[%s, %s, %d]\n", 
                    rs.getString("_KEY"), 
                    rs.getString("name"), 
                    rs.getInt("zip"));
                
            }
        }
    }

    private static void insertRow(Connection conn, String key, String name, int zip) throws SQLException {
        try(PreparedStatement stmnt = conn.prepareStatement(INSERT_QRY)) {
            stmnt.setString(1, key);
            stmnt.setString(2, name);
            stmnt.setInt(3, zip);

            stmnt.execute();
        }

    }

    private static void deleteAll(Connection conn) throws SQLException {
        try (PreparedStatement stmnt = conn.prepareStatement("DELETE FROM ADDR.Address")) {
            stmnt.execute();
        }
    }
}
