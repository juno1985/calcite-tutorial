package com.example.driver;

import org.apache.calcite.avatica.remote.Driver;

/**
 * Tableau JDBC driver for Calcite.
 * This driver extends Avatica's remote JDBC driver and is used to connect to the Calcite server.
 * 
 * Connection URL format:
 * jdbc:avatica:remote:url=http://[SERVER_HOST]:8765;serialization=PROTOBUF
 */
public class TableauDriver extends Driver {
    static {
        new TableauDriver().register();
    }

    public TableauDriver() {
        super();
    }
}
