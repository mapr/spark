package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
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

    super.init(filterConfigWrapper);
  }
}
