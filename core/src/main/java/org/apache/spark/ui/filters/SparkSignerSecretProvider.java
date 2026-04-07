package org.apache.spark.ui.filters;

import jakarta.servlet.ServletContext;
import org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationFilter;
import org.apache.spark.ui.filters.hadoop_auth_copy.SignerSecretProvider;

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
