package org.apache.spark.ui.filters.hadoop_auth_copy;

/**
 * Exception thrown when an authentication error occurs.
 */
public class AuthenticationException extends Exception {

  static final long serialVersionUID = 0L;

  public AuthenticationException(Throwable cause) {
    super(cause);
  }

  public AuthenticationException(String msg) {
    super(msg);
  }

  public AuthenticationException(String msg, Throwable cause) {
    super(msg, cause);
  }
}
