package com.example.driver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class TableauDriverTest {
    public static void main(String[] args) {
        String url = "jdbc:avatica:remote:url=http://localhost:8765;serialization=PROTOBUF";
        
        try {
            // Register the driver
            Class.forName("com.example.driver.TableauDriver");
            
            // Create connection
            System.out.println("Connecting to server...");
            Connection connection = DriverManager.getConnection(url);
            System.out.println("Connected successfully!");
            
            // Create statement
            Statement statement = connection.createStatement();
            
            // Execute query
            String sql = "VALUES (1 + 2)";
            System.out.println("Executing query: " + sql);
            ResultSet rs = statement.executeQuery(sql);
            
            // Print results
            while (rs.next()) {
                System.out.println("Result: " + rs.getInt(1));
            }
            
            // Clean up
            rs.close();
            statement.close();
            connection.close();
            System.out.println("Test completed successfully!");
            
        } catch (Exception e) {
            System.err.println("Error: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
