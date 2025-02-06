package com.common;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class PropertiesManager {
    Logger logger = LoggerFactory.getLogger(PropertiesManager.class);
    private static final Properties properties = new Properties();
    private static PropertiesManager instance;

    private PropertiesManager(){
        try {
            FileInputStream fis = new FileInputStream("src/main/resources/application.properties");
            properties.load(fis);
        }catch (IOException err){
            logger.error("Failed to load properties file " + err);
        }
    }

    public static PropertiesManager getInstance(){
        if (instance == null){
            instance = new PropertiesManager();
        }
        return instance;
    }

    public String getProperty(String key){
        return properties.getProperty(key);
    }

    public int getIntProperty(String key){
        return Integer.parseInt(getProperty(key));
    }

    public boolean getBooleanProperty(String key){
        return Boolean.getBoolean(getProperty(key));
    }

}
