package com.datafreezer.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertiesManager {
    private static final Logger logger = LoggerFactory.getLogger(PropertiesManager.class);
    private static final Properties properties = new Properties();
    private static PropertiesManager instance;

    private PropertiesManager() {
        try (InputStream is = getClass().getClassLoader().getResourceAsStream("application.properties")) {
            if (is == null) {
                throw new IOException("application.properties not found in classpath");
            }
            properties.load(is);
        } catch (IOException err) {
            logger.error("Failed to load properties file", err);
        }
    }

    public static PropertiesManager getInstance() {
        if (instance == null) {
            instance = new PropertiesManager();
        }
        return instance;
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public int getIntProperty(String key) {
        String value = getProperty(key);
        if (value == null) {
            throw new NumberFormatException("Property '" + key + "' is missing or null");
        }
        return Integer.parseInt(value);
    }

    public boolean getBooleanProperty(String key) {
        String value = getProperty(key);
        return Boolean.parseBoolean(value);
    }
}
