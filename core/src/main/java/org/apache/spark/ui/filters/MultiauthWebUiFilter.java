package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;
import org.apache.hadoop.security.authentication.util.SignerSecretProvider;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.List;

public class MultiauthWebUiFilter extends AuthenticationFilter {

  private List<String> allowedResources = new ArrayList<>();

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

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
          throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    Cookie[] cookies = httpRequest.getCookies();
    boolean isLoggedOut = checkForCookie(cookies, "logout");
    if (isLoggedOut) {
      cleanCookieForResponse(cookies, httpResponse);
      httpResponse.sendRedirect("/login");
      return;
    }

    if (allowedResources.contains(httpRequest.getRequestURI())) {
      chain.doFilter(httpRequest, httpResponse);
    } else {
      super.doFilter(httpRequest, httpResponse, chain);
    }
  }

  private void cleanCookieForResponse(Cookie[] cookies, HttpServletResponse httpResponse) {
    if (cookies != null) {
      for (Cookie cookie : cookies) {
        cookie.setValue("");
        cookie.setPath("/");
        cookie.setMaxAge(0);
        httpResponse.addCookie(cookie);
      }
    }
  }

  private boolean checkForCookie(Cookie[] cookies, String cookieName) {
    boolean isCookiePresent = false;

    if (cookies != null) {
      for (Cookie cookie : cookies) {
        if (cookieName.equals(cookie.getName())) {
          isCookiePresent = true;
          break;
        }
      }
    }
    return isCookiePresent;
  }
}
