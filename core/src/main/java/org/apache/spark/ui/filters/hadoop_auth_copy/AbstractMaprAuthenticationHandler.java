package org.apache.spark.ui.filters.hadoop_auth_copy;

import org.apache.hadoop.security.authentication.client.AbstractMaprAuthenticator;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

public abstract class AbstractMaprAuthenticationHandler extends MultiMechsAuthenticationHandler {

  public static final String TYPE = "maprauth";

  @Override
  public abstract void init(Properties config) throws ServletException;

  public abstract AuthenticationToken maprAuthenticate(
      HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException;

  @Override
  public void destroy() {}

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    if (request.getHeader(KerberosAuthenticator.AUTHORIZATION) != null) {
      return maprAuthenticate(request, response);
    }
    return null;
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, AbstractMaprAuthenticator.NEGOTIATE);
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if (authorization != null && authorization.startsWith(AbstractMaprAuthenticator.NEGOTIATE)) {
      return this;
    }
    return null;
  }

  @Override
  public String getAuthorizationHeaderName() {
    return AbstractMaprAuthenticator.NEGOTIATE;
  }
}
