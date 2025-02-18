package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;

import javax.servlet.ServletContext;
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
