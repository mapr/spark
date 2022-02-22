package org.apache.spark.network.scram;

import javax.security.auth.callback.*;
import javax.security.sasl.AuthorizeCallback;
import javax.security.sasl.RealmCallback;

/**
 * Callback handler for Sasl clients. The callbacks required for the SASL mechanism
 * configured for the client should be supported by this callback handler. See
 * <a href="https://docs.oracle.com/javase/8/docs/technotes/guides/security/sasl/sasl-refguide.html">Java SASL API</a>
 * for the list of SASL callback handlers required for each SASL mechanism.
 */
public class ScramClientCallbackHandler implements CallbackHandler {

  private final String password;
  private final String userName;

  public ScramClientCallbackHandler(String userName, String password) {
    this.userName = userName;
    this.password = password;
  }

  @Override
  public void handle(Callback[] callbacks) throws UnsupportedCallbackException {
    for (Callback callback : callbacks) {
      if (callback instanceof NameCallback) {
        NameCallback nc = (NameCallback) callback;
        if (userName != null) {
          nc.setName(userName);
        } else
          nc.setName(nc.getDefaultName());
      } else if (callback instanceof PasswordCallback) {
        if (password != null) {
          ((PasswordCallback) callback).setPassword(password.toCharArray());
        } else {
          String errorMessage = "Could not login: the client is being asked for a password," +
              " but the client code does not currently support obtaining a password from the user.";
          throw new UnsupportedCallbackException(callback, errorMessage);
        }
      } else if (callback instanceof RealmCallback) {
        RealmCallback rc = (RealmCallback) callback;
        rc.setText(rc.getDefaultText());
      } else if (callback instanceof AuthorizeCallback) {
        AuthorizeCallback ac = (AuthorizeCallback) callback;
        String authId = ac.getAuthenticationID();
        String authzId = ac.getAuthorizationID();
        ac.setAuthorized(authId.equals(authzId));
        if (ac.isAuthorized())
          ac.setAuthorizedID(authzId);
      } else {
        throw new UnsupportedCallbackException(callback, "Unrecognized SASL ClientCallback");
      }
    }
  }
}
