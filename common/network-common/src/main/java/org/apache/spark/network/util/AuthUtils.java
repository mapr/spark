package org.apache.spark.network.util;

public class AuthUtils {
    public static boolean isMD5() {
        return !FipsUtils.isFips();
    }

    public static boolean isSCRAM() {
        return FipsUtils.isFips();
    }
}
