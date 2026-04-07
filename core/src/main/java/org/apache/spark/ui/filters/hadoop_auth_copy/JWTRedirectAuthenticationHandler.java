package org.apache.spark.ui.filters.hadoop_auth_copy;

import com.auth0.jwt.JWT;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.classification.VisibleForTesting;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationFilter.ACTION_PARAM;

public class JWTRedirectAuthenticationHandler extends MultiMechsAuthenticationHandler {
  private static final Logger LOG = LoggerFactory.getLogger(JWTRedirectAuthenticationHandler.class);

  public static final String AUTHENTICATION_PROVIDER_URL = "authentication.provider.url";
  public static final String JWT_CLIENT_ID = "jwt.client.id";
  public static final String JWT_CLIENT_SECRET = "jwt.client.secret";
  private static final String REDIRECT_URI_QUERY_PARAM = "redirect_uri=";
  private String authenticationProviderUrl = null;
  private String clientId = null;
  private final String delimiter = "&";

  private static final String CODE = "code";

  @Override
  public void init(Properties config) throws ServletException {
    authenticationProviderUrl =
        config.getProperty(AUTHENTICATION_PROVIDER_URL, SsoConfigurationUtil.getInstance().getClientIssuer());
    if (authenticationProviderUrl == null) {
      throw new ServletException(
          "Authentication provider URL must not be null - configure: "
              + AUTHENTICATION_PROVIDER_URL);
    }
    if (authenticationProviderUrl.endsWith("/")) {
      authenticationProviderUrl = authenticationProviderUrl.substring(0, authenticationProviderUrl.length() - 1);
    }

    clientId = config.getProperty(JWT_CLIENT_ID, SsoConfigurationUtil.getInstance().getClientId());
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String serializedJWT = getJWTFromCookie(request);
    if (serializedJWT == null && request.getParameter(CODE) == null) {
      String loginURL = constructLoginURL(request);
      LOG.debug("Sending redirect to: {}", loginURL);
      response.sendRedirect(loginURL);
    } else if (serializedJWT == null && request.getParameter(CODE) != null) {
      String jsonJWT = getJWTTokenFromCode(request.getParameter(CODE), request);
      ObjectMapper mapper = new ObjectMapper();
      JsonNode node = mapper.readTree(jsonJWT);
      String jwtStr = node.get("access_token").asText();
      DecodedJWT jwt = JWT.decode(jwtStr);
      if (JWTUtils.validateToken(jwt)) {
        String userName = jwt.getClaim(SsoConfigurationUtil.getInstance().getUserAttrName()).asString();
        token = new AuthenticationToken(userName, userName, getType());
        token.setJWTExpires(jwt.getExpiresAt().getTime());
        token.setJWTBasedToken(true);
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("Can't add token to cookie, because validating failed.");
        response.sendRedirect(loginURL);
      }
    } else if (serializedJWT != null) {
      String userName = null;
      DecodedJWT jwtToken = JWT.decode(serializedJWT);
      boolean valid = JWTUtils.validateToken(jwtToken);
      if (valid) {
        userName = jwtToken.getClaim(SsoConfigurationUtil.getInstance().getUserAttrName()).asString();
      } else {
        LOG.warn("jwtToken failed validation: {}", jwtToken.getToken());
      }
      if (valid) {
        LOG.debug("Issuing AuthenticationToken for user.");
        token = new AuthenticationToken(userName, userName, getType());
      } else {
        String loginURL = constructLoginURL(request);
        LOG.info("token validation failed - sending redirect to: {}", loginURL);
        response.sendRedirect(loginURL);
      }
    } else {
      LOG.info("JWT can't be found in cookies or get from the authentication server");
    }
    return token;
  }

  public String getJWTTokenFromCode(String code, HttpServletRequest request) throws IOException {
    StringBuilder content = new StringBuilder();
    String ui2 = request.getParameter("ui2") != null ? "ui2" : "";
    String form =
        "grant_type=authorization_code"
            + "&client_id="
            + URLEncoder.encode(SsoConfigurationUtil.getInstance().getClientId(), StandardCharsets.UTF_8.name())
            + "&code="
            + URLEncoder.encode(code, StandardCharsets.UTF_8.name())
            + "&client_secret="
            + URLEncoder.encode(
                SsoConfigurationUtil.getInstance().getClientSecret(), StandardCharsets.UTF_8.name())
            + "&redirect_uri="
            + URLEncoder.encode(
                request.getRequestURL().toString() + ui2 + "?" + ACTION_PARAM + "=processCode",
                StandardCharsets.UTF_8.name());

    HttpURLConnection connection = (HttpURLConnection) new URL(getTokenUrl()).openConnection();
    connection.setRequestMethod("POST");
    connection.setDoOutput(true);
    connection.setRequestProperty("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8");
    connection.setRequestProperty("Accept", "application/json");

    byte[] payload = form.getBytes(StandardCharsets.UTF_8);
    connection.setFixedLengthStreamingMode(payload.length);
    try (OutputStream os = connection.getOutputStream()) {
      os.write(payload);
    }

    InputStream responseStream =
        connection.getResponseCode() >= 400 ? connection.getErrorStream() : connection.getInputStream();
    if (responseStream == null) {
      responseStream = connection.getInputStream();
    }
    try (BufferedReader br =
        new BufferedReader(new InputStreamReader(responseStream, StandardCharsets.UTF_8))) {
      String line;
      while ((line = br.readLine()) != null) {
        content.append(line);
        content.append(System.lineSeparator());
      }
    }

    return content.toString();
  }

  public String getTokenUrl() {
    return authenticationProviderUrl + "/protocol/openid-connect/token";
  }

  private String getJWTFromCookie(HttpServletRequest request) {
    Cookie[] cookies = request.getCookies();
    String cookieName = SsoConfigurationUtil.getInstance().getCookieName();
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          LOG.info("{} cookie has been found and is being processed", cookieName);
          return cookie.getValue();
        }
      }
    }
    return null;
  }

  public String getAuthUrl() {
    return authenticationProviderUrl + "/protocol/openid-connect/auth";
  }

  @VisibleForTesting
  String constructLoginURL(HttpServletRequest request) {
    return getAuthUrl()
        + "?"
        + "response_type=code"
        + delimiter
        + "client_id="
        + clientId
        + delimiter
        + "scope=openid"
        + delimiter
        + REDIRECT_URI_QUERY_PARAM
        + JWTUtils.constructURLWithHostname(request.getRequestURL().toString());
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, "Bearer realm=\"master\"");
  }
}
