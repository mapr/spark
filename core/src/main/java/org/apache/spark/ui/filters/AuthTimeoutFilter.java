package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.server.AuthenticationFilter;

import javax.servlet.*;
import javax.servlet.http.Cookie;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;

public class AuthTimeoutFilter extends AuthenticationFilter {

  private int TIMEOUT_DURATION = 30; // 30 minutes
  private int ABSOLUTE_TIMEOUT_DURATION = 600; // 600
  private final String LAST_ACTIVITY_COOKIE_NAME = "lastActivity";
  private final String SESSION_START_COOKIE_NAME = "sessionStart";
  private final String LOGOUT_COOKIE_NAME = "logout";

  @Override
  public void init(FilterConfig filterConfig) throws ServletException {
    TIMEOUT_DURATION = Integer.parseInt(filterConfig.getInitParameter("inactiveTimeout"));
    ABSOLUTE_TIMEOUT_DURATION = Integer.parseInt(filterConfig.getInitParameter("absoluteTimeout"));
  }

  @Override
  public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
    HttpServletRequest httpRequest = (HttpServletRequest) request;
    HttpServletResponse httpResponse = (HttpServletResponse) response;

    Cookie[] cookies = httpRequest.getCookies();
    Cookie lastActivityCookie = null;
    Cookie sessionStartCookie = null;
    Cookie logoutCookie = null;

    if (cookies != null) {
      for (Cookie cookie : cookies) {
        switch (cookie.getName()) {
          case LAST_ACTIVITY_COOKIE_NAME:
            lastActivityCookie = cookie;
            break;
          case SESSION_START_COOKIE_NAME:
            sessionStartCookie = cookie;
            break;
          case LOGOUT_COOKIE_NAME:
            logoutCookie = cookie;
            break;
        }
      }
    }

    long currentTime = System.currentTimeMillis();

    if (logoutCookie != null && sessionStartCookie != null && lastActivityCookie != null) {
      invalidateCookie(httpResponse, SESSION_START_COOKIE_NAME);
      invalidateCookie(httpResponse, LAST_ACTIVITY_COOKIE_NAME);
      httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"session_logout\"");
      httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Logged out");
      return;
    }
    invalidateCookie(httpResponse, LOGOUT_COOKIE_NAME);

    if (lastActivityCookie != null && sessionStartCookie != null) {
      try {
        long lastActivityTime = Long.parseLong(lastActivityCookie.getValue());
        long sessionStartTime = Long.parseLong(sessionStartCookie.getValue());

        if ((currentTime - lastActivityTime > (long) TIMEOUT_DURATION * 60 * 1000) ||
                (currentTime - sessionStartTime > (long) ABSOLUTE_TIMEOUT_DURATION * 60 * 1000)) {

          invalidateCookie(httpResponse, LAST_ACTIVITY_COOKIE_NAME);
          invalidateCookie(httpResponse, SESSION_START_COOKIE_NAME);

          httpResponse.setHeader("WWW-Authenticate", "Basic realm=\"session_timeout\"");
          httpResponse.sendError(HttpServletResponse.SC_UNAUTHORIZED, "Session timed out");

          return;
        }
      } catch (NumberFormatException e) {
        e.printStackTrace();
      }
    }

    httpResponse.addCookie(createSecureCookie(LAST_ACTIVITY_COOKIE_NAME, Long.toString(currentTime)));

    if (sessionStartCookie == null) {
      httpResponse.addCookie(createSecureCookie(SESSION_START_COOKIE_NAME, Long.toString(currentTime)));
    }

    chain.doFilter(request, response);
  }

  private Cookie createSecureCookie(String name, String value) {
    Cookie cookie = new Cookie(name, value);
    cookie.setPath("/");
    cookie.setHttpOnly(true);
    cookie.setSecure(true);
    return cookie;
  }

  private void invalidateCookie(HttpServletResponse response, String name) {
    Cookie cookie = new Cookie(name, "");
    cookie.setPath("/");
    cookie.setMaxAge(0);
    cookie.setHttpOnly(true);
    cookie.setSecure(true);
    response.addCookie(cookie);
  }

  @Override
  public void destroy() {
    // Cleanup code if needed
  }

}