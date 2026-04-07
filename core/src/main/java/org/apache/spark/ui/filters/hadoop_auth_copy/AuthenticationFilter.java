package org.apache.spark.ui.filters.hadoop_auth_copy;

import com.google.common.net.HttpHeaders;
import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletContext;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletRequestWrapper;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.hadoop.security.authentication.client.AuthenticatedURL;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.JWTUtils;
import org.apache.hadoop.security.authentication.util.SsoConfigurationUtil;
import org.json.simple.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnknownHostException;
import java.security.Principal;
import java.text.SimpleDateFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TimeZone;

public class AuthenticationFilter implements Filter {
  private static Logger LOG = LoggerFactory.getLogger(AuthenticationFilter.class);

  public static final String CONFIG_PREFIX = "config.prefix";
  public static final String AUTH_TYPE = "type";
  public static final String SIGNATURE_SECRET = "signature.secret";
  public static final String SIGNATURE_SECRET_FILE = SIGNATURE_SECRET + ".file";
  public static final String AUTH_TOKEN_MAX_INACTIVE_INTERVAL = "token.max-inactive-interval";
  public static final String AUTH_TOKEN_VALIDITY = "token.validity";
  public static final String COOKIE_DOMAIN = "cookie.domain";
  public static final String COOKIE_PATH = "cookie.path";
  public static final String COOKIE_PERSISTENT = "cookie.persistent";
  public static final String SIGNER_SECRET_PROVIDER = "signer.secret.provider";
  public static final String SIGNER_SECRET_PROVIDER_ATTRIBUTE = "signer.secret.provider.object";
  public static final String SSO_LOGIN_COOKIE = "isSsoLogin";
  public static final String ACTION_PARAM = "action";

  private Properties config;
  private Signer signer;
  private SignerSecretProvider secretProvider;
  private AuthenticationHandler authHandler;
  private long maxInactiveInterval;
  private long validity;
  private String cookieDomain;
  private String cookiePath;
  private boolean isCookiePersistent;
  private boolean destroySecretProvider;
  protected String configPrefix;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    configPrefix = filterConfig.getInitParameter(CONFIG_PREFIX);
    configPrefix = (configPrefix != null) ? configPrefix + "." : "";
    config = getConfiguration(configPrefix, filterConfig);
    String authHandlerName = config.getProperty(AUTH_TYPE, null);
    if (authHandlerName == null) {
      throw new ServletException("Authentication type must be specified");
    }
    String authHandlerClassName = authHandlerName;

    maxInactiveInterval = Long.parseLong(config.getProperty(AUTH_TOKEN_MAX_INACTIVE_INTERVAL, "-1"));
    if (maxInactiveInterval > 0) {
      maxInactiveInterval *= 1000;
    }
    validity = Long.parseLong(config.getProperty(AUTH_TOKEN_VALIDITY, "36000")) * 1000;
    initializeSecretProvider(filterConfig);
    initializeAuthHandler(authHandlerClassName, filterConfig);

    String domainName = null;
    try {
      InetAddress localHost = InetAddress.getLocalHost();
      String fqdn = localHost.getCanonicalHostName();
      if (fqdn != null && !fqdn.isEmpty() && fqdn.contains(".")) {
        domainName = fqdn.substring(fqdn.indexOf("."));
      }
    } catch (UnknownHostException e) {
      // ignore
    }

    cookieDomain = config.getProperty(COOKIE_DOMAIN, domainName);
    cookiePath = config.getProperty(COOKIE_PATH, "/");
    isCookiePersistent = Boolean.parseBoolean(config.getProperty(COOKIE_PERSISTENT, "false"));
  }

  protected void initializeAuthHandler(String authHandlerClassName, FilterConfig filterConfig)
      throws ServletException {
    try {
      Class<?> klass = Thread.currentThread().getContextClassLoader().loadClass(authHandlerClassName);
      authHandler = (AuthenticationHandler) klass.newInstance();
      authHandler.init(config);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException ex) {
      throw new ServletException(ex);
    }
  }

  protected void initializeSecretProvider(FilterConfig filterConfig) throws ServletException {
    secretProvider =
        (SignerSecretProvider) filterConfig.getServletContext().getAttribute(SIGNER_SECRET_PROVIDER_ATTRIBUTE);
    if (secretProvider == null) {
      try {
        secretProvider = constructSecretProvider(filterConfig.getServletContext(), config, false);
        destroySecretProvider = true;
      } catch (Exception ex) {
        throw new ServletException(ex);
      }
    }
    signer = new Signer(secretProvider);
  }

  public static SignerSecretProvider constructSecretProvider(
      ServletContext ctx, Properties config, boolean disallowFallbackToRandomSecretProvider) throws Exception {
    String name = config.getProperty(SIGNER_SECRET_PROVIDER, "file");
    long validity = Long.parseLong(config.getProperty(AUTH_TOKEN_VALIDITY, "36000")) * 1000;

    if (!disallowFallbackToRandomSecretProvider
        && "file".equals(name)
        && config.getProperty(SIGNATURE_SECRET_FILE) == null) {
      name = "random";
    }

    SignerSecretProvider provider;
    if ("file".equals(name)) {
      provider = new FileSignerSecretProvider();
      try {
        provider.init(config, ctx, validity);
      } catch (Exception e) {
        if (!disallowFallbackToRandomSecretProvider) {
          LOG.warn(
              "Unable to initialize FileSignerSecretProvider, falling back to use random secrets. Reason: {}",
              e.getMessage());
          provider = new RandomSignerSecretProvider();
          provider.init(config, ctx, validity);
        } else {
          throw e;
        }
      }
    } else if ("random".equals(name)) {
      provider = new RandomSignerSecretProvider();
      provider.init(config, ctx, validity);
    } else if ("zookeeper".equals(name)) {
      provider = new ZKSignerSecretProvider();
      provider.init(config, ctx, validity);
    } else {
      provider =
          (SignerSecretProvider)
              Thread.currentThread().getContextClassLoader().loadClass(name).newInstance();
      provider.init(config, ctx, validity);
    }
    return provider;
  }

  protected Properties getConfiguration() {
    return config;
  }

  protected AuthenticationHandler getAuthenticationHandler() {
    return authHandler;
  }

  protected boolean isRandomSecret() {
    return secretProvider.getClass() == RandomSignerSecretProvider.class;
  }

  protected boolean isCustomSignerSecretProvider() {
    Class<?> clazz = secretProvider.getClass();
    return clazz != FileSignerSecretProvider.class
        && clazz != RandomSignerSecretProvider.class
        && clazz != ZKSignerSecretProvider.class;
  }

  protected long getMaxInactiveInterval() {
    return maxInactiveInterval / 1000;
  }

  protected long getValidity() {
    return validity / 1000;
  }

  protected String getCookieDomain() {
    return cookieDomain;
  }

  protected String getCookiePath() {
    return cookiePath;
  }

  protected boolean isCookiePersistent() {
    return isCookiePersistent;
  }

  @Override
  public void destroy() {
    if (authHandler != null) {
      authHandler.destroy();
      authHandler = null;
    }
    if (secretProvider != null && destroySecretProvider) {
      secretProvider.destroy();
      secretProvider = null;
    }
  }

  protected Properties getConfiguration(String configPrefix, FilterConfig filterConfig)
      throws ServletException {
    Properties props = new Properties();
    Enumeration<?> names = filterConfig.getInitParameterNames();
    while (names.hasMoreElements()) {
      String name = (String) names.nextElement();
      if (name.startsWith(configPrefix)) {
        String value = filterConfig.getInitParameter(name);
        props.put(name.substring(configPrefix.length()), value);
      }
    }
    return props;
  }

  protected String getRequestURL(HttpServletRequest request) {
    StringBuffer sb = request.getRequestURL();
    if (request.getQueryString() != null) {
      sb.append("?").append(request.getQueryString());
    }
    return sb.toString();
  }

  protected AuthenticationToken getToken(HttpServletRequest request)
      throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String tokenStr = null;
    Cookie[] cookies = request.getCookies();
    boolean isJWTBasedToken = false;
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookie.getName().equals(getCookieTokenName())) {
          tokenStr = cookie.getValue();
          if (tokenStr.isEmpty()) {
            throw new AuthenticationException("Unauthorized access");
          }
          try {
            tokenStr = signer.verifyAndExtract(tokenStr);
          } catch (SignerException ex) {
            throw new AuthenticationException(ex);
          }
        }
        if (cookie.getName().equals(SSO_LOGIN_COOKIE) && cookie.getValue().equals("1")) {
          isJWTBasedToken = true;
        }
      }
    }
    if (tokenStr != null) {
      token = AuthenticationToken.parse(tokenStr);
      boolean match = verifyTokenType(getAuthenticationHandler(), token);
      if (!match) {
        throw new AuthenticationException("Invalid AuthenticationToken type");
      }
      if (token.isExpired()) {
        throw new AuthenticationException("AuthenticationToken expired");
      }
      if (isJWTBasedToken) {
        token.setJWTBasedToken(true);
      }
    }
    return token;
  }

  protected boolean verifyTokenType(AuthenticationHandler handler, AuthenticationToken token) {
    if (!(handler instanceof CompositeAuthenticationHandler)) {
      return handler.getType().equals(token.getType());
    }
    boolean match = false;
    Collection<String> tokenTypes = ((CompositeAuthenticationHandler) handler).getTokenTypes();
    for (String tokenType : tokenTypes) {
      if (tokenType.equals(token.getType())) {
        match = true;
        break;
      }
    }
    return match;
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain filterChain)
      throws IOException, ServletException {
    boolean unauthorizedResponse = true;
    int errCode = HttpServletResponse.SC_UNAUTHORIZED;
    AuthenticationException authenticationEx = null;
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    boolean isHttps = "https".equals(httpRequest.getScheme());
    if (checkSsoEnableRequest(httpRequest)) {
      if (SsoConfigurationUtil.getInstance().getClientIssuer().isEmpty()) {
        httpResponse.setStatus(HttpServletResponse.SC_FORBIDDEN);
      } else {
        httpResponse.setStatus(HttpServletResponse.SC_OK);
      }
      return;
    }
    try {
      boolean newToken = false;
      AuthenticationToken token;
      try {
        token = getToken(httpRequest);
        if (LOG.isDebugEnabled()) {
          LOG.debug("Got token {} from httpRequest {}", token, getRequestURL(httpRequest));
        }
      } catch (AuthenticationException ex) {
        LOG.warn("AuthenticationToken ignored: {}", ex.getMessage());
        authenticationEx = ex;
        token = null;
      }
      if (authHandler.managementOperation(token, httpRequest, httpResponse)) {
        if (token == null) {
          if (checkRedirectToLogin(httpRequest)) {
            httpResponse.addHeader(HttpHeaders.LOCATION, getLoginURL(httpRequest));
            httpResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
            return;
          } else if (httpRequest.getParameter(ACTION_PARAM) != null
              && httpRequest.getParameter(ACTION_PARAM).equalsIgnoreCase("initSso")) {
            Map<String, String> ssoMap = new HashMap<>();
            ssoMap.put("loginURL", constructLoginURL(httpRequest));
            JSONObject ssoJson = new JSONObject(ssoMap);
            httpResponse.setContentType("application/json");
            httpResponse.getWriter().write(ssoJson.toJSONString());
            httpResponse.setStatus(HttpServletResponse.SC_OK);
            return;
          }
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Request [{}] triggering authentication. handler: {}",
                getRequestURL(httpRequest),
                authHandler.getClass());
          }
          token = authHandler.authenticate(httpRequest, httpResponse);
          if (token != null && token != AuthenticationToken.ANONYMOUS) {
            if (token.getMaxInactives() > 0) {
              token.setMaxInactives(System.currentTimeMillis() + getMaxInactiveInterval() * 1000);
            }
            if (token.getExpires() != 0) {
              token.setExpires(System.currentTimeMillis() + getValidity() * 1000);
            }
          }
          newToken = true;
        }
        if (token != null) {
          if (request.getParameter(ACTION_PARAM) != null
              && request.getParameter(ACTION_PARAM).equals("logout")) {
            Cookie ssoCookie = null;
            if (token.isJWTBasedToken()) {
              ssoCookie = new Cookie(SSO_LOGIN_COOKIE, "");
              ssoCookie.setMaxAge(0);
              ssoCookie.setPath(getCookiePath());
            }
            Cookie cleanAuthCookie = new Cookie(getCookieTokenName(), "");
            cleanAuthCookie.setMaxAge(0);
            cleanAuthCookie.setPath(getCookiePath());
            String domain = getCookieDomain();
            if (domain != null && httpRequest.getRequestURL().toString().contains(domain)) {
              if (domain.startsWith(".")) {
                domain = domain.substring(1);
              }
              if (ssoCookie != null) {
                ssoCookie.setDomain(domain);
              }
              cleanAuthCookie.setDomain(domain);
            }
            if (ssoCookie != null) {
              httpResponse.addCookie(ssoCookie);
            }
            httpResponse.addCookie(cleanAuthCookie);
            if (token.isJWTBasedToken()) {
              httpResponse.addHeader(HttpHeaders.LOCATION, getLogoutUrl());
              httpResponse.setStatus(HttpServletResponse.SC_OK);
            } else {
              httpResponse.addHeader(HttpHeaders.LOCATION, getLoginURL(httpRequest));
              httpResponse.setStatus(HttpServletResponse.SC_TEMPORARY_REDIRECT);
            }
            return;
          }
          unauthorizedResponse = false;
          if (LOG.isDebugEnabled()) {
            LOG.debug(
                "Request [{}] user [{}] authenticated",
                getRequestURL(httpRequest),
                token.getUserName());
          }
          final AuthenticationToken authToken = token;
          httpRequest =
              new HttpServletRequestWrapper(httpRequest) {
                @Override
                public String getAuthType() {
                  return authToken.getType();
                }

                @Override
                public String getRemoteUser() {
                  return authToken.getUserName();
                }

                @Override
                public Principal getUserPrincipal() {
                  return (authToken != AuthenticationToken.ANONYMOUS) ? authToken : null;
                }
              };

          if (!newToken && !isCookiePersistent() && getMaxInactiveInterval() > 0) {
            token.setMaxInactives(System.currentTimeMillis() + getMaxInactiveInterval() * 1000);
            token.setExpires(token.getExpires());
            newToken = true;
          }
          if (newToken && !token.isExpired() && token != AuthenticationToken.ANONYMOUS) {
            String signedToken = signer.sign(token.toString());
            createAuthCookie(
                getCookieTokenName(),
                httpResponse,
                httpRequest,
                signedToken,
                getCookieDomain(),
                getCookiePath(),
                token.getExpires(),
                isCookiePersistent(),
                isHttps,
                token.getJWTExpires());
            if (token.isJWTBasedToken()) {
              createSSOLoginCookie(
                  httpResponse,
                  httpRequest,
                  getCookieDomain(),
                  getCookiePath(),
                  token.getExpires(),
                  isHttps,
                  token.getJWTExpires());
            }
          }
          doFilter(filterChain, httpRequest, httpResponse);
        }
      } else {
        if (LOG.isDebugEnabled()) {
          LOG.debug(
              "managementOperation returned false for request {}. token: {}",
              getRequestURL(httpRequest),
              token);
        }
        unauthorizedResponse = false;
      }
    } catch (AuthenticationException ex) {
      errCode = HttpServletResponse.SC_FORBIDDEN;
      authenticationEx = ex;
      if (LOG.isDebugEnabled()) {
        LOG.debug("Authentication exception: " + ex.getMessage(), ex);
      } else {
        LOG.warn("Authentication exception: " + ex.getMessage());
      }
    }
    if (unauthorizedResponse) {
      if (!httpResponse.isCommitted()) {
        createAuthCookie(
            getCookieTokenName(),
            httpResponse,
            httpRequest,
            "",
            getCookieDomain(),
            getCookiePath(),
            0,
            isCookiePersistent(),
            isHttps,
            -1);
        if ((errCode == HttpServletResponse.SC_UNAUTHORIZED)
            && (!httpResponse.containsHeader(KerberosAuthenticator.WWW_AUTHENTICATE)
                && !httpResponse.containsHeader(
                    KerberosAuthenticator.WWW_AUTHENTICATE.toLowerCase(Locale.ROOT)))) {
          errCode = HttpServletResponse.SC_FORBIDDEN;
        }
        String reason = authenticationEx == null ? "Authentication required" : authenticationEx.getMessage();
        httpResponse.sendError(errCode, reason);
      }
    }
  }

  private boolean checkSsoEnableRequest(HttpServletRequest httpRequest) {
    String userAgent = httpRequest.getHeader(HttpHeaders.USER_AGENT);
    return userAgent != null
        && userAgent.startsWith("Mozilla/5.0")
        && httpRequest.getParameter(ACTION_PARAM) != null
        && httpRequest.getParameter(ACTION_PARAM).equalsIgnoreCase("ssoEnable");
  }

  private boolean checkRedirectToLogin(HttpServletRequest httpRequest) {
    String userAgent = httpRequest.getHeader(HttpHeaders.USER_AGENT);
    return httpRequest.getHeader(KerberosAuthenticator.AUTHORIZATION) == null
        && userAgent != null
        && userAgent.startsWith("Mozilla/5.0")
        && !httpRequest.getRequestURI().replace("/", "").equals("login")
        && httpRequest.getParameter(ACTION_PARAM) == null;
  }

  private void createSSOLoginCookie(
      HttpServletResponse res,
      HttpServletRequest req,
      String cookieDomain,
      String cookiePath,
      long expires,
      boolean isHttps,
      long jwtExpires) {
    Cookie ssoLoginCookie = new Cookie(SSO_LOGIN_COOKIE, "1");
    if (cookieDomain != null && (req == null || req.getRequestURL().toString().contains(cookieDomain))) {
      if (cookieDomain.startsWith(".")) {
        cookieDomain = cookieDomain.substring(1);
      }
      ssoLoginCookie.setDomain(cookieDomain);
    }
    if (cookiePath != null) {
      ssoLoginCookie.setPath(cookiePath);
    }
    if (expires >= 0 || jwtExpires >= 0) {
      long exp = Math.min(expires, jwtExpires);
      int maxAge = (int) (exp - System.currentTimeMillis()) / 1000;
      ssoLoginCookie.setMaxAge(maxAge);
    }
    if (isHttps) {
      ssoLoginCookie.setSecure(true);
    }
    ssoLoginCookie.setHttpOnly(true);
    res.addCookie(ssoLoginCookie);
  }

  protected void doFilter(FilterChain filterChain, HttpServletRequest request, HttpServletResponse response)
      throws IOException, ServletException {
    filterChain.doFilter(request, response);
  }

  public static void createAuthCookie(
      String name,
      HttpServletResponse resp,
      HttpServletRequest req,
      String token,
      String domain,
      String path,
      long expires,
      boolean isCookiePersistent,
      boolean isSecure,
      long jwtExp) {
    StringBuilder sb = new StringBuilder(name).append("=");

    if (token != null && token.length() > 0) {
      sb.append("\"").append(token).append("\"");
    }

    if (path != null) {
      sb.append("; Path=").append(path);
    }

    if (domain != null && (req == null || req.getRequestURL().toString().contains(domain))) {
      if (domain.startsWith(".")) {
        domain = domain.substring(1);
      }
      sb.append("; Domain=").append(domain);
    }
    if ((expires >= 0 && isCookiePersistent) || jwtExp >= 0) {
      Date date;
      if (jwtExp < expires && jwtExp >= 0) {
        date = new Date(jwtExp);
      } else {
        date = new Date(expires);
      }
      SimpleDateFormat df = new SimpleDateFormat("EEE, dd-MMM-yyyy HH:mm:ss zzz", Locale.US);
      df.setTimeZone(TimeZone.getTimeZone("GMT"));
      sb.append("; Expires=").append(df.format(date));
    }

    if (isSecure) {
      sb.append("; Secure");
    }

    sb.append("; HttpOnly");
    resp.addHeader("Set-Cookie", sb.toString());
  }

  String constructLoginURL(HttpServletRequest request) {
    String delimiter = "&";
    String ui2 = request.getParameter("ui2") != null ? "ui2" : "";
    return getAuthUrl()
        + "?"
        + "response_type=code"
        + delimiter
        + "client_id="
        + SsoConfigurationUtil.getInstance().getClientId()
        + delimiter
        + "scope=openid"
        + delimiter
        + "redirect_uri="
        + JWTUtils.constructURLWithHostname(request.getRequestURL().toString())
        + ui2
        + "?"
        + ACTION_PARAM
        + "=processCode";
  }

  public String getAuthUrl() {
    return SsoConfigurationUtil.getInstance().getClientIssuer() + "/protocol/openid-connect/auth";
  }

  public String getLogoutUrl() {
    return SsoConfigurationUtil.getInstance().getClientIssuer() + "/protocol/openid-connect/logout";
  }

  private String getLoginURL(HttpServletRequest httpRequest) {
    String redirect;
    try {
      URI origin = new URI(httpRequest.getRequestURL().toString());
      redirect = origin.getScheme() + "://" + origin.getHost() + ":" + origin.getPort() + "/login";
    } catch (URISyntaxException e) {
      throw new RuntimeException(e);
    }
    return redirect;
  }

  public String getCookieTokenName() {
    return AuthenticatedURL.AUTH_COOKIE;
  }
}
