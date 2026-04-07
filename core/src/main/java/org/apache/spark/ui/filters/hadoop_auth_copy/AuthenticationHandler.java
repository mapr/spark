package org.apache.spark.ui.filters.hadoop_auth_copy;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.spark.ui.filters.HttpConstants;

import java.io.IOException;
import java.util.Properties;

public interface AuthenticationHandler {

  String WWW_AUTHENTICATE = HttpConstants.WWW_AUTHENTICATE_HEADER;

  String getType();

  void init(Properties config) throws ServletException;

  void destroy();

  boolean managementOperation(AuthenticationToken token,
                              HttpServletRequest request,
                              HttpServletResponse response) throws IOException, AuthenticationException;

  AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException;
}
