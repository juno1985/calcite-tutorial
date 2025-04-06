package com.example.server;

import org.apache.calcite.avatica.server.HttpServer;
import org.apache.calcite.avatica.remote.Driver.Serialization;
import org.apache.calcite.avatica.remote.LocalService;
import org.apache.calcite.avatica.Meta;
import org.apache.calcite.avatica.remote.Service;

public class CalciteAvaticaServer {
    private static final int PORT = 8765;

    public static void main(String[] args) throws Exception {
        // Create Meta implementation
        Meta meta = new CalciteMetaImpl(CalciteConnectionFactory.createConnection());
        
        // Create local service
        LocalService service = new LocalService(meta);
        
        // Start HTTP server
        HttpServer server = new HttpServer.Builder<Service>()
            .withPort(PORT)
            .withHandler(service, Serialization.PROTOBUF)
            .build();
        
        server.start();
        
        System.out.println("Server started on port " + PORT);
        System.out.println("JDBC URL: jdbc:avatica:remote:url=http://localhost:" + PORT + ";serialization=PROTOBUF");
        
        // Add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Shutting down server...");
            server.stop();
        }));

        // Keep the server running
        server.join();
    }
}
