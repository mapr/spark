package org.apache.spark.ui.filters;

import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationFilter;
import org.apache.spark.ui.filters.hadoop_auth_copy.SignerSecretProvider;

import java.io.IOException;
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

    servicePort = httpRequest.getServerPort();

    if (isStaticDirectoryRequest(httpRequest)) {
      httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    if (allowedResources.contains(httpRequest.getRequestURI())) {
      chain.doFilter(httpRequest, httpResponse);
    } else {
      super.doFilter(httpRequest, httpResponse, chain);
    }
  }

  @Override
  public String getCookieTokenName(){
    return "hadoop.auth." + servicePort;
  }
}
