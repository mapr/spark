package org.apache.spark.ui.filters.hadoop_auth_copy;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;
import java.util.HashMap;
import java.util.Map;

public class JaasConfiguration extends Configuration {
  private final Configuration baseConfig = Configuration.getConfiguration();
  private final AppConfigurationEntry[] entry;
  private final String entryName;

  public JaasConfiguration(String entryName, String principal, String keytab) {
    this.entryName = entryName;
    Map<String, String> options = new HashMap<>();
    options.put("keyTab", keytab);
    options.put("principal", principal);
    options.put("useKeyTab", "true");
    options.put("storeKey", "true");
    options.put("useTicketCache", "false");
    options.put("refreshKrb5Config", "true");
    String jaasEnvVar = System.getenv("HADOOP_JAAS_DEBUG");
    if ("true".equalsIgnoreCase(jaasEnvVar)) {
      options.put("debug", "true");
    }
    entry =
        new AppConfigurationEntry[] {
          new AppConfigurationEntry(
              getKrb5LoginModuleName(), AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, options)
        };
  }

  @Override
  public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
    return (entryName.equals(name))
        ? entry
        : ((baseConfig != null) ? baseConfig.getAppConfigurationEntry(name) : null);
  }

  private String getKrb5LoginModuleName() {
    if (System.getProperty("java.vendor").contains("IBM")) {
      return "com.ibm.security.auth.module.Krb5LoginModule";
    }
    return "com.sun.security.auth.module.Krb5LoginModule";
  }
}
