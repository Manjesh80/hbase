package com.manjesh.hbase;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.junit4.SpringRunner;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Author: mg153v (Manjesh Gowda). Creation Date: 3/27/2017.
 */
public class PhoenixCRUDTests {

    @Test
    public void testPhoenixCrud() throws Exception {
        Connection connection = null;
        Statement statement = null;
        ResultSet rs = null;
        PreparedStatement ps = null;
        try {
            Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
            // Connect to the database
            //connection = DriverManager.getConnection("jdbc:phoenix:192.168.99.101:2181:/hbase-unsecure");
            connection = DriverManager.getConnection("jdbc:phoenix:192.168.99.101:2181:/hbase");
            System.out.println("Connection established....");
            // Create a JDBC statement
            statement = connection.createStatement();
            // Execute our statements
            statement.executeUpdate(
                    "create table if not exists user (id INTEGER NOT NULL PRIMARY KEY, d.first_name VARCHAR,d.last_name VARCHAR)");
            statement.executeUpdate("upsert into user values (1,'John','Mayer')");
            statement.executeUpdate("upsert into user values (2,'Eva','Peters')");
            connection.commit();
            // Query for selecting records from table
            ps = connection.prepareStatement("select * from user");
            rs = ps.executeQuery();
            System.out.println("Table Values");
            while (rs.next()) {
                Integer id = rs.getInt(1);
                String name = rs.getString(2);
                System.out.println("**********   " + name + "      ***********");
                System.out.println("\tRow: " + id + " = " + name);
            }
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                try {
                    ps.close();
                } catch (Exception e) {
                }
            }
            if (rs != null) {
                try {
                    rs.close();
                } catch (Exception e) {
                }
            }
            if (statement != null) {
                try {
                    statement.close();
                } catch (Exception e) {
                }
            }
            if (connection != null) {
                try {
                    connection.close();
                } catch (Exception e) {
                }
            }
        }
    }
}

