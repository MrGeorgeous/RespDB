package com.itmo.java.basics.config;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Properties;


/**
 * Класс, отвечающий за подгрузку данных из конфигурационного файла формата .properties
 */
public class ConfigLoader {

    private String configPath;

    /**
     * По умолчанию читает из server.properties
     */
    public ConfigLoader() {
        this.configPath = "server.properties";
    }

    /**
     * @param name Имя конфикурационного файла, откуда читать
     */
    public ConfigLoader(String name) {
        this.configPath = name;
    }

    /**
     * Считывает конфиг из указанного в конструкторе файла.
     * Если не удалось считать из заданного файла, или какого-то конкретно значения не оказалось,
     * то используют дефолтные значения из {@link DatabaseConfig} и {@link ServerConfig}
     * <br/>
     * Читаются: "kvs.workingPath", "kvs.host", "kvs.port" (но в конфигурационном файле допустимы и другие проперти)
     */
    public DatabaseServerConfig readConfig() {

        Properties properties = new Properties();
        try {
            InputStream stream = this.getClass().getClassLoader().getResourceAsStream(configPath);
            properties.load(stream);
            stream.close();
        } catch (Exception e) {
            try {
                FileInputStream stream = new FileInputStream(configPath);
                properties.load(stream);
            } catch (Exception ex) {
                // ignoring non-existent configuration file
            }
        }

        DatabaseConfig databaseConfig;
        String workingPath = properties.getProperty("kvs.workingPath");
        if (workingPath != null) {
            databaseConfig = new DatabaseConfig(workingPath);
        } else {
            databaseConfig = new DatabaseConfig(DatabaseConfig.DEFAULT_WORKING_PATH);
        }

        HashMap<String, String> serverSettings = new HashMap<>() {{
            put("kvs.host", ServerConfig.DEFAULT_HOST);
            put("kvs.port", String.valueOf(ServerConfig.DEFAULT_PORT));
        }};

        for (String field : serverSettings.keySet()) {
            String value = properties.getProperty(field);
            if (value != null) {
                serverSettings.put(field, value);
            }
        }

        ServerConfig serverConfig = new ServerConfig(serverSettings.get("kvs.host"), Integer.valueOf(serverSettings.get("kvs.port")));
        return new DatabaseServerConfig(serverConfig, databaseConfig);

    }
}
