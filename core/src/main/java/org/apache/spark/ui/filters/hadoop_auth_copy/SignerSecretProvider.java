package org.apache.spark.ui.filters.hadoop_auth_copy;

import jakarta.servlet.ServletContext;

import java.util.Properties;

public abstract class SignerSecretProvider {

  public abstract void init(Properties config, ServletContext servletContext,
                            long tokenValidity) throws Exception;

  public void destroy() {
  }

  public abstract byte[] getCurrentSecret();

  public abstract byte[][] getAllSecrets();
}
