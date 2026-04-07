package org.apache.spark.ui.filters.hadoop_auth_copy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

public class JWTAuthHandler extends MultiMechsAuthenticationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(JWTAuthHandler.class);
  private static final String BEARER_AUTH = "Bearer";

  @Override
  public void init(Properties config) throws ServletException {}

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken authToken = null;
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    if (authorization != null && authorization.toLowerCase().startsWith(BEARER_AUTH.toLowerCase())) {
      try {
        String[] splitHeader = authorization.split(" ");
        if (splitHeader.length != 2) {
          LOG.error(
              "Too many parts in auth header (expected exactly 2, but was {}): {}",
              splitHeader.length,
              authorization);
          response.setStatus(HttpServletResponse.SC_FORBIDDEN);
          return null;
        }
        String jwtStr = splitHeader[1];
        String userName = null;
        DecodedJWT jwtToken = JWT.decode(jwtStr);
        boolean valid = JWTUtils.validateToken(jwtToken);
        if (valid) {
          userName = jwtToken.getClaim(SsoConfigurationUtil.getInstance().getUserAttrName()).asString();
        } else {
          LOG.warn("jwtToken failed validation: {}", jwtToken.getToken());
        }
        if (valid) {
          LOG.debug("Issuing AuthenticationToken for user.");
          authToken = new AuthenticationToken(userName, userName, getType());
        } else {
          LOG.error("Token validation failed.");
          response.setStatus(HttpServletResponse.SC_FORBIDDEN);
          return null;
        }
      } catch (Exception e) {
        LOG.warn("AUTH FAILURE: {}", e.toString());
      }
    } else {
      LOG.error("Unexpected or empty auth header: {}", authorization);
      throw new AuthenticationException("Unsupported auth scheme in header: " + authorization);
    }

    return authToken;
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Bearer realm=\"master\"");
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if (authorization != null && authorization.startsWith(BEARER_AUTH)) {
      return this;
    }
    return null;
  }

  @Override
  public String getAuthorizationHeaderName() {
    return BEARER_AUTH;
  }
}
