package org.apache.spark.ui.filters;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;
import java.security.Principal;

public class RemoteUserWrapper extends HttpServletRequestWrapper {

  private String user;
  private HttpServletRequest realRequest;

  public RemoteUserWrapper(String user, HttpServletRequest request) {
    super(request);
    this.user = user;
    this.realRequest = request;
  }

  @Override
  public String getRemoteUser() {
    if (this.user == null) {
      return realRequest.getRemoteUser();
    }
    return user;
  }

  @Override
  public Principal getUserPrincipal() {
    if (this.user == null) {
      return realRequest.getUserPrincipal();
    }

    return () -> user;
  }
}
