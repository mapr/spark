package org.apache.spark.ui.filters;

import jakarta.servlet.ServletContext;
import com.mapr.web.security.hadoop.jakartaauth.AuthenticationFilter;
import com.mapr.web.security.hadoop.jakartaauth.SignerSecretProvider;

import java.util.Properties;

public class SparkSignerSecretProvider {

    private static volatile SignerSecretProvider instance;

    private SparkSignerSecretProvider() {}

    public static SignerSecretProvider getInstance(ServletContext servletContext, Properties config, boolean disallowFallbackToRandom) {

        if (instance == null) {
            synchronized (SparkSignerSecretProvider.class) {
                if (instance == null) {
                    try {
                        instance = AuthenticationFilter.constructSecretProvider(servletContext, config, disallowFallbackToRandom);
                    } catch (Exception e) {
                        throw new RuntimeException("Failed to get or construct SecretProvider", e);
                    }
                }
            }
        }
        return instance;
    }
}
