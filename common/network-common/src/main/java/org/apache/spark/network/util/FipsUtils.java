package org.apache.spark.network.util;

import java.security.Provider;
import java.security.Security;

public class FipsUtils {
    public static boolean isFips() {
        Provider[] providers = Security.getProviders();
        for (Provider provider : providers) {
            if (provider.getName().toLowerCase().contains("fips")) return true;
        }
        return false;
    }
}
