package org.apache.spark.network.scram;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.scram.CredentialCache;
import org.apache.hadoop.security.scram.ScramCredential;
import org.apache.hadoop.security.scram.ScramFormatter;
import org.apache.hadoop.security.scram.ScramMechanism;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.Marker;
import org.slf4j.MarkerFactory;

import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;

public class ScramUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(ScramUtils.class);
    private static final Marker FATAL = MarkerFactory.getMarker("FATAL");
    public static final String SCRAM_SHA_256 = "SCRAM-SHA-256";
    private static final String SCRAM_CONFIG = "scram/scram-site.xml";

    public static String getScramUserName() {
        try {
            return UserGroupInformation.getLoginUser().getUserName();
        } catch (IOException ex) {
            LOGGER.error("Exception while getting user for SCRAM", ex);
            ex.printStackTrace();
        }
        return null;
    }

    public static CredentialCache createCache() {
        CredentialCache credentialCache = new CredentialCache();
        try {
            credentialCache.createCache(SCRAM_SHA_256, ScramCredential.class);
            ScramFormatter formatter = new ScramFormatter(ScramMechanism.SCRAM_SHA_256);
            ScramCredential generatedCred = formatter.generateCredential(createSecretScram(), 4096);
            CredentialCache.Cache<ScramCredential> sha256Cache = credentialCache.cache(SCRAM_SHA_256, ScramCredential.class);
            sha256Cache.put(getScramUserName(), generatedCred);
        } catch (NoSuchAlgorithmException ex) {
            LOGGER.error(FATAL, "Can't find " + SCRAM_SHA_256 + " algorithm.");
        }
        return credentialCache;
    }

    public static String createSecretScram() {
        try {
            Configuration configuration = new Configuration();
            configuration.addResource(SCRAM_CONFIG);
            return Arrays.toString(configuration.getPassword("scram.password"));
        } catch (IOException ex) {
            LOGGER.error("Exception while getting password for SCRAM", ex);
            ex.printStackTrace();
        }
        return null;
    }
}
