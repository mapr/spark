package org.apache.spark.ui.filters.hadoop_auth_copy;

/**
 * Authentication token that also acts as the request principal.
 */
public class AuthenticationToken extends AuthToken {
  public static final AuthenticationToken ANONYMOUS = new AuthenticationToken();

  private AuthenticationToken() {
    super();
  }

  private AuthenticationToken(AuthToken token) {
    super(token.getUserName(), token.getName(), token.getType());
    setMaxInactives(token.getMaxInactives());
    setExpires(token.getExpires());
  }

  public AuthenticationToken(String userName, String principal, String type) {
    super(userName, principal, type);
  }

  @Override
  public void setMaxInactives(long maxInactives) {
    if (this != AuthenticationToken.ANONYMOUS) {
      super.setMaxInactives(maxInactives);
    }
  }

  @Override
  public void setExpires(long expires) {
    if (this != AuthenticationToken.ANONYMOUS) {
      super.setExpires(expires);
    }
  }

  @Override
  public boolean isExpired() {
    return super.isExpired();
  }

  public static AuthenticationToken parse(String tokenStr) throws AuthenticationException {
    return new AuthenticationToken(AuthToken.parse(tokenStr));
  }
}
