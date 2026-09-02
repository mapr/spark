package org.apache.spark.ui.filters;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpServletResponseWrapper;
import com.mapr.web.security.hadoop.jakartaauth.AuthenticationFilter;
import com.mapr.web.security.hadoop.jakartaauth.SignerSecretProvider;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

public class MultiauthWebUiFilter extends AuthenticationFilter {

  private List<String> allowedResources = new ArrayList<>();
  private int servicePort;

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    FilterConfigWrapper filterConfigWrapper = new FilterConfigWrapper();
    Enumeration<String> parameterNames = filterConfig.getInitParameterNames();
    filterConfigWrapper.setFilterName(filterConfig.getFilterName());
    filterConfigWrapper.setServletContext(filterConfig.getServletContext());
    while (parameterNames.hasMoreElements()) {
      String key = parameterNames.nextElement();
      String value = filterConfig.getInitParameter(key);
      filterConfigWrapper.setInitParameter(key, value);
    }
    filterConfigWrapper.setInitParameter(
            AuthenticationFilter.AUTH_TYPE,
            "org.apache.spark.ui.filters.hadoop_auth_copy.MultiMechsAuthenticationHandler");
    initializeAuthTypes(filterConfig, filterConfigWrapper);

    String kerberosDisable = filterConfig.getInitParameter("kerberosDisable");
    if (kerberosDisable != null) {
      filterConfigWrapper.setInitParameter("kerberos.disable", kerberosDisable);
    }

    SignerSecretProvider signerProvider = SparkSignerSecretProvider.getInstance(filterConfig.getServletContext(),
            getConfiguration("", filterConfigWrapper), false);
    filterConfigWrapper.getServletContext().setAttribute(AuthenticationFilter.SIGNER_SECRET_PROVIDER_ATTRIBUTE,
            signerProvider);

    String allowedResourcesParam = filterConfig.getInitParameter("allowedResources");
    if (allowedResourcesParam != null) {
      allowedResources = new ArrayList<>(Arrays.asList(allowedResourcesParam.split(",")));
    }

    super.init(filterConfigWrapper);
  }

  private void initializeAuthTypes(FilterConfig filterConfig, FilterConfigWrapper filterConfigWrapper) {
    if (filterConfig.getInitParameter("type0") != null) {
      return;
    }

    String authTypes = filterConfig.getInitParameter("auth.types");
    if (authTypes != null && !authTypes.trim().isEmpty()) {
      List<String> parsedTypes = new ArrayList<>();
      for (String rawType : authTypes.split(",")) {
        String normalized = rawType == null ? "" : rawType.trim();
        if (!normalized.isEmpty()) {
          parsedTypes.add(normalized.toLowerCase());
        }
      }
      if (!parsedTypes.isEmpty()) {
        for (int i = 0; i < parsedTypes.size(); i++) {
          filterConfigWrapper.setInitParameter("type" + i, parsedTypes.get(i));
        }
        return;
      }
    }

    // Prevent MultiMechs from auto-enabling maprauth/kerberos defaults
    filterConfigWrapper.setInitParameter("type0", "basic");
    filterConfigWrapper.setInitParameter("type1", "bearer");
  }

  private boolean isStaticDirectoryRequest(HttpServletRequest req) {
    String path = req.getRequestURI();
    return path.matches("(.*/)?static/?$")
            || path.matches("(.*/)?static/.+/$");
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;
    boolean rmProxyRequest = isRmProxyRequest(httpRequest);

    servicePort = httpRequest.getServerPort();

    if (isStaticDirectoryRequest(httpRequest)) {
      httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    if (allowedResources.contains(httpRequest.getRequestURI())) {
      chain.doFilter(httpRequest, httpResponse);
    } else {
      if (rmProxyRequest) {
        RmProxyResponseWrapper responseWrapper = new RmProxyResponseWrapper(httpResponse);
        super.doFilter(httpRequest, responseWrapper, chain);
        if (responseWrapper.shouldForceBasicChallenge() && !httpResponse.isCommitted()) {
          httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"Spark UI\"");
          httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Authentication required");
        }
      } else {
        super.doFilter(httpRequest, httpResponse, chain);
      }
    }
  }

  private boolean isRmProxyRequest(HttpServletRequest request) {
    String forwardedContext = request.getHeader("X-Forwarded-Context");
    if (forwardedContext != null && forwardedContext.contains("/proxy/")) {
      return true;
    }
    String proxyBase = System.getProperty("spark.ui.proxyBase");
    if (proxyBase != null && proxyBase.contains("/proxy/")) {
      return true;
    }
    String appProxyBase = System.getenv("APPLICATION_WEB_PROXY_BASE");
    return appProxyBase != null && appProxyBase.contains("/proxy/");
  }

  private boolean isLoginRedirectLocation(String location) {
    if (location == null || location.isEmpty()) {
      return false;
    }
    if (location.equals("/login") || location.equals("/login/") ||
            location.startsWith("/login?") || location.startsWith("/login/?")) {
      return true;
    }
    if (location.startsWith("http://") || location.startsWith("https://")) {
      try {
        URI uri = new URI(location);
        String path = uri.getPath();
        return path != null && (path.equals("/login") || path.equals("/login/"));
      } catch (URISyntaxException e) {
        return false;
      }
    }
    return location.equals("login") || location.equals("login/");
  }

  private class RmProxyResponseWrapper extends HttpServletResponseWrapper {
    private boolean forceBasicChallenge;

    RmProxyResponseWrapper(HttpServletResponse response) {
      super(response);
    }

    @Override
    public void sendRedirect(String location) throws IOException {
      if (isLoginRedirectLocation(location)) {
        forceBasicChallenge = true;
        return;
      }
      super.sendRedirect(location);
    }

    @Override
    public void setHeader(String name, String value) {
      if ("Location".equalsIgnoreCase(name) && isLoginRedirectLocation(value)) {
        forceBasicChallenge = true;
        return;
      }
      super.setHeader(name, value);
    }

    @Override
    public void addHeader(String name, String value) {
      if ("Location".equalsIgnoreCase(name) && isLoginRedirectLocation(value)) {
        forceBasicChallenge = true;
        return;
      }
      super.addHeader(name, value);
    }

    boolean shouldForceBasicChallenge() {
      return forceBasicChallenge;
    }
  }

  @Override
  public String getCookieTokenName(){
    return "hadoop.auth." + servicePort;
  }
}
