package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Enumeration;

public class MultiauthWebUiFilter extends AuthenticationFilter {

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

    filterConfigWrapper.setInitParameter(AuthenticationFilter.AUTH_TYPE,
      "org.apache.hadoop.security.authentication.server.MultiMechsAuthenticationHandler");

    String kerberosDisable = filterConfig.getInitParameter("kerberosDisable");
    if (kerberosDisable != null) {
      filterConfigWrapper.setInitParameter("kerberos.disable", kerberosDisable);
    }
    super.init(filterConfigWrapper);
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

    if (isStaticDirectoryRequest(httpRequest)) {
      httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN);
      return;
    }

    chain.doFilter(httpRequest, httpResponse);
  }
}
