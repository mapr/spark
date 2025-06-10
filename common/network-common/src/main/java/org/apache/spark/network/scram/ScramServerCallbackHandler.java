package org.apache.spark.network.scram;

import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.scram.CredentialCache;
import org.apache.hadoop.security.scram.ScramCredential;
import org.apache.hadoop.security.scram.ScramCredentialCallback;

import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import java.io.IOException;

public class ScramServerCallbackHandler implements CallbackHandler {

    private final CredentialCache.Cache<ScramCredential> credentialCache;

    public ScramServerCallbackHandler(CredentialCache.Cache<ScramCredential> credentialCache) {
        this.credentialCache = credentialCache;
    }

    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
        for (Callback callback : callbacks) {
            if (callback instanceof NameCallback)
                ((NameCallback) callback).getDefaultName();
            else if (callback instanceof ScramCredentialCallback) {
                ((ScramCredentialCallback) callback).scramCredential(
                        credentialCache.get(UserGroupInformation.getLoginUser().getUserName()));
            } else
                throw new UnsupportedCallbackException(callback);
        }
    }
}
