package com.example.server;

import org.apache.calcite.jdbc.Driver;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.util.Properties;
import java.io.File;

public class CalciteConnectionFactory {
    private static final String MODEL_PATH = "F:/fabric/calcite_tableau_v1/calcite/calcite-avatica-server/src/main/resources/model.yaml";

    public static Connection createConnection() throws SQLException {
        // Verify model file exists
        File modelFile = new File(MODEL_PATH);
        System.out.println("\nInitializing Calcite connection:");
        System.out.println("Loading model from: " + modelFile.getAbsolutePath());
        if (!modelFile.exists()) {
            throw new SQLException("Model file not found: " + MODEL_PATH);
        }
        System.out.println("Model file exists and is readable: " + modelFile.canRead());
        System.out.println("Model file size: " + modelFile.length() + " bytes");

        Properties info = new Properties();
        info.setProperty("model", modelFile.getAbsolutePath());
        info.setProperty("caseSensitive", "true");  // Change to true to respect case sensitivity
        
        System.out.println("\nCreating connection with properties:");
        info.forEach((k, v) -> System.out.println("  " + k + "=" + v));
        
        Driver driver = new Driver();
        Connection connection = driver.connect("jdbc:calcite:", info);
        
        if (connection == null) {
            throw new SQLException("Unable to create Calcite connection");
        }

        // Debug: List available tables
        System.out.println("\nAvailable tables in connection:");
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE", "VIEW"});
        while (tables.next()) {
            System.out.println("  Found: " + 
                             tables.getString("TABLE_SCHEM") + "." + 
                             tables.getString("TABLE_NAME") + 
                             " (type: " + tables.getString("TABLE_TYPE") + ")");
        }
        
        System.out.println("Successfully created Calcite connection\n");
        return connection;
    }
}
