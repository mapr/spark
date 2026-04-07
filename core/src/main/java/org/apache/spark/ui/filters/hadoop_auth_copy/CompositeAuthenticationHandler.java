package org.apache.spark.ui.filters.hadoop_auth_copy;

import java.util.Collection;

public interface CompositeAuthenticationHandler extends AuthenticationHandler {
  Collection<String> getTokenTypes();
}
