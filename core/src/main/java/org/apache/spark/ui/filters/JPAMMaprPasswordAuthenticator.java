package org.apache.spark.ui.filters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.callback.*;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.io.IOException;

/**
 * Copy of mapr core password authenticator based on jpamLogin module
 */
public final class JPAMMaprPasswordAuthenticator {

    private static final Logger LOGGER = LoggerFactory.getLogger(JPAMMaprPasswordAuthenticator.class);
    public static final String PASSWORD_PAM_JAAS_CONFIGURATION = "jpamLogin";

    public static boolean authenticate(final String name, final String password) {
        try {
            JAASCallbackHandler handler = new JAASCallbackHandler(name, password);
            LoginContext loginContext = new LoginContext(PASSWORD_PAM_JAAS_CONFIGURATION, handler);
            loginContext.login();
            return true;
        } catch (LoginException e) {
            LOGGER.error("Failed to auth user " + name, e);
            return false;
        }
    }

    static class JAASCallbackHandler implements CallbackHandler {

        private final String name;
        private final String password;

        public JAASCallbackHandler(String name, String password) {
            this.name = name;
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback callback : callbacks) {
                if (callback instanceof NameCallback) {
                    ((NameCallback) callback).setName(this.name);
                } else if (callback instanceof PasswordCallback) {
                    ((PasswordCallback) callback).setPassword(this.password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(callback, "unsupported callback type: " + callback.getClass().getCanonicalName());
                }
            }
        }
    }

}
