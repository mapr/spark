/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hive.service.auth;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.HiveMetaStore;
import org.apache.hadoop.hive.metastore.HiveMetaStore.HMSHandler;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.shims.HadoopShims.KerberosNameShim;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.hive.thrift.DBTokenStore;
import org.apache.hadoop.hive.thrift.HiveDelegationTokenManager;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.thrift.HadoopThriftAuthBridge.Server.ServerMode;
import org.apache.hadoop.security.SecurityUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.hive.service.auth.PlainSaslHelper;
import org.apache.hive.service.cli.HiveSQLException;
import org.apache.hive.service.cli.thrift.ThriftCLIService;
import org.apache.thrift.TProcessorFactory;
import org.apache.thrift.transport.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.*;
import javax.security.auth.login.LoginException;
import javax.security.sasl.Sasl;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.UnknownHostException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.*;

/**
 * This class helps in some aspects of authentication. It creates the proper Thrift classes for the
 * given configuration as well as helps with authenticating requests.
 */
public class HiveAuthFactory {
  private static final Logger LOG = LoggerFactory.getLogger(HiveAuthFactory.class);

  public enum AuthTypes {
    NOSASL("NOSASL"),
    NONE("NONE"),
    LDAP("LDAP"),
    KERBEROS("KERBEROS"),
    CUSTOM("CUSTOM"),
    PAM("PAM"),
    MAPRSASL("MAPRSASL");

    private final String authType;

    AuthTypes(String authType) {
      this.authType = authType;
    }

    public String getAuthName() {
      return authType;
    }

  }

  private HadoopThriftAuthBridge.Server saslServer;
  private String authTypeStr;
  private final String transportMode;
  private final HiveConf conf;
  private HiveDelegationTokenManager delegationTokenManager = null;

  public static final String HS2_PROXY_USER = "hive.server2.proxy.user";
  public static final String HS2_CLIENT_TOKEN = "hiveserver2ClientToken";
  public static final String BCFKS_KEYSTORE_TYPE = "bcfks";

  private static Method getKeytab = null;
  static {
    Class<?> clz = UserGroupInformation.class;

    try {
      getKeytab = clz.getDeclaredMethod("getKeytab");
      getKeytab.setAccessible(true);
    } catch(NoSuchMethodException nme) {
      LOG.debug("Cannot find private method \"getKeytab\" in class:" +
        UserGroupInformation.class.getCanonicalName(), nme);
      getKeytab = null;
    }
  }

  public HiveAuthFactory(HiveConf conf) throws TTransportException, IOException {
    this.conf = conf;
    transportMode = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE);
    authTypeStr = conf.getVar(HiveConf.ConfVars.HIVE_SERVER2_AUTHENTICATION);
    boolean isAuthTypeSecured = "KERBEROS".equalsIgnoreCase(authTypeStr) || "MAPRSASL".equalsIgnoreCase(authTypeStr);

    // In http mode we use NOSASL as the default auth type
    if ("http".equalsIgnoreCase(transportMode)) {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NOSASL.getAuthName();
      }
    } else {
      if (authTypeStr == null) {
        authTypeStr = AuthTypes.NONE.getAuthName();
      }
      if (isAuthTypeSecured || ("PAM".equalsIgnoreCase(authTypeStr)
              && ShimLoader.getHadoopShims().isSecurityEnabled())) {

        saslServer = ShimLoader.getHadoopThriftAuthBridge()
                .createServer(conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB),
                        conf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL));

          // start delegation token manager
          delegationTokenManager = new HiveDelegationTokenManager();
          try {
            // rawStore is only necessary for DBTokenStore
            Object rawStore = null;
            String tokenStoreClass = conf.getVar(
                    HiveConf.ConfVars.METASTORE_CLUSTER_DELEGATION_TOKEN_STORE_CLS);

            if (tokenStoreClass.equals(DBTokenStore.class.getName())) {
              HMSHandler baseHandler = new HiveMetaStore.HMSHandler(
                      "new db based metaserver", conf, true);
              rawStore = baseHandler.getMS();
            }

            delegationTokenManager.startDelegationTokenSecretManager(
                    conf, rawStore, ServerMode.HIVESERVER2);
            saslServer.setSecretManager(delegationTokenManager.getSecretManager());
          } catch (MetaException | IOException e) {
            throw new TTransportException("Failed to start token manager", e);
          }

      } else {
        saslServer = null;
      }
    }
  }

  public Map<String, String> getSaslProperties() {
    Map<String, String> saslProps = new HashMap<String, String>();
    SaslQOP saslQOP = SaslQOP.fromString(conf.getVar(ConfVars.HIVE_SERVER2_THRIFT_SASL_QOP));
    saslProps.put(Sasl.QOP, saslQOP.toString());
    saslProps.put(Sasl.SERVER_AUTH, "true");
    return saslProps;
  }

  public TTransportFactory getAuthTransFactory() throws LoginException {
    TTransportFactory transportFactory;
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName()) ||
        authTypeStr.equalsIgnoreCase(AuthTypes.MAPRSASL.getAuthName())) {
      try {
        transportFactory = saslServer.createTransportFactory(getSaslProperties());
      } catch (TTransportException e) {
        throw new LoginException(e.getMessage());
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NONE.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.LDAP.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.PAM.getAuthName())) {
      if (ShimLoader.getHadoopShims().isSecurityEnabled()) {
        try {
          transportFactory = saslServer.createTransportFactory(getSaslProperties());
        } catch (TTransportException e) {
          throw new LoginException(e.getMessage());
        }
        PlainSaslHelper.addPlainDefinitionToFactory(authTypeStr, transportFactory, saslServer);
      } else {
        transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
      }
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.NOSASL.getAuthName())) {
      transportFactory = new TTransportFactory();
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.CUSTOM.getAuthName())) {
      transportFactory = PlainSaslHelper.getPlainTransportFactory(authTypeStr);
    } else {
      throw new LoginException("Unsupported authentication type " + authTypeStr);
    }
    return transportFactory;
  }

  /**
   * Returns the thrift processor factory for HiveServer2 running in binary mode
   * @param service
   * @return
   * @throws LoginException
   */
  public TProcessorFactory getAuthProcFactory(ThriftCLIService service) throws LoginException {
    if (authTypeStr.equalsIgnoreCase(AuthTypes.KERBEROS.getAuthName())) {
      return KerberosSaslHelper.getKerberosProcessorFactory(saslServer, service);
    } else if (authTypeStr.equalsIgnoreCase(AuthTypes.MAPRSASL.getAuthName())) {
      return MapRSecSaslHelper.getProcessorFactory(saslServer, service);
    } else {
      return PlainSaslHelper.getPlainProcessorFactory(service);
    }
  }

  public String getRemoteUser() {
    return saslServer == null ? null : saslServer.getRemoteUser();
  }

  public String getIpAddress() {
    if (saslServer == null || saslServer.getRemoteAddress() == null) {
      return null;
    } else {
      return saslServer.getRemoteAddress().getHostAddress();
    }
  }

  // Perform kerberos login using the hadoop shim API if the configuration is available
  public static void loginFromKeytab(HiveConf hiveConf) throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_KERBEROS_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 Kerberos principal or keytab is not correctly configured");
    } else {
      UserGroupInformation.loginUserFromKeytab(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  // Perform SPNEGO login using the hadoop shim API if the configuration is available
  public static UserGroupInformation loginFromSpnegoKeytabAndReturnUGI(HiveConf hiveConf)
    throws IOException {
    String principal = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_PRINCIPAL);
    String keyTabFile = hiveConf.getVar(ConfVars.HIVE_SERVER2_SPNEGO_KEYTAB);
    if (principal.isEmpty() || keyTabFile.isEmpty()) {
      throw new IOException("HiveServer2 SPNEGO principal or keytab is not correctly configured");
    } else {
      return UserGroupInformation.loginUserFromKeytabAndReturnUGI(SecurityUtil.getServerPrincipal(principal, "0.0.0.0"), keyTabFile);
    }
  }

  public static TTransport getSocketTransport(String host, int port, int loginTimeout) {
    return new TSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout)
          throws TTransportException {
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout);
  }

  public static TTransport getSSLSocket(String host, int port, int loginTimeout,
                                        String trustStorePath, String trustStorePassWord) throws TTransportException {
    TSSLTransportFactory.TSSLTransportParameters params =
            new TSSLTransportFactory.TSSLTransportParameters();
    params.setTrustStore(trustStorePath, trustStorePassWord);
    params.requireClientAuth(true);
    return TSSLTransportFactory.getClientSocket(host, port, loginTimeout, params);
  }

  //Create SSL Socket for MAPRSASL connection. Ignore SSL trusted servers as MAPRSASL perform encryption by itself
  public static TTransport getTrustAllSSLSocket(String host, int port, int loginTimeout) throws TTransportException {
    TrustManager[] trustAllCerts = new TrustManager[]{
            new X509ExtendedTrustManager() {
              @Override
              public void checkClientTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
              }
              @Override
              public void checkServerTrusted(X509Certificate[] x509Certificates, String s) throws CertificateException {
              }
              @Override
              public X509Certificate[] getAcceptedIssuers() {
                return null;
              }
              @Override
              public void checkClientTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
              }
              @Override
              public void checkServerTrusted(X509Certificate[] x509Certificates, String s, Socket socket) throws CertificateException {
              }
              @Override
              public void checkClientTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
              }
              @Override
              public void checkServerTrusted(X509Certificate[] x509Certificates, String s, SSLEngine sslEngine) throws CertificateException {
              }
            }
    };
    SSLSocket socket;
    try {
      SSLContext sslContext = SSLContext.getInstance("SSL");
      sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
      SSLSocketFactory factory = sslContext.getSocketFactory();
      socket = (SSLSocket) factory.createSocket(host, port);
      socket.setSoTimeout(loginTimeout);
    } catch (NoSuchAlgorithmException | IOException | KeyManagementException e) {
      throw new TTransportException("Couldn't create Trust All SSL socket", e);
    }
    return new TSocket(socket);
  }

  public static TServerSocket getServerSocket(String hiveHost, int portNum)
    throws TTransportException {
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    return new TServerSocket(serverAddress);
  }

  public static TServerSocket getServerSSLSocket(String hiveHost, int portNum, String keyStorePath,
                                                 String keyStorePassWord, List<String> sslVersionBlacklist) throws TTransportException,
          UnknownHostException {
    TSSLTransportFactory.TSSLTransportParameters params =
            new TSSLTransportFactory.TSSLTransportParameters();
    params.setKeyStore(keyStorePath, keyStorePassWord);
    InetSocketAddress serverAddress;
    if (hiveHost == null || hiveHost.isEmpty()) {
      // Wildcard bind
      serverAddress = new InetSocketAddress(portNum);
    } else {
      serverAddress = new InetSocketAddress(hiveHost, portNum);
    }
    TServerSocket thriftServerSocket =
        TSSLTransportFactory.getServerSocket(portNum, 0, serverAddress.getAddress(), params);
    if (thriftServerSocket.getServerSocket() instanceof SSLServerSocket) {
      List<String> sslVersionBlacklistLocal = new ArrayList<String>();
      for (String sslVersion : sslVersionBlacklist) {
        sslVersionBlacklistLocal.add(sslVersion.trim().toLowerCase(Locale.ROOT));
      }
      SSLServerSocket sslServerSocket = (SSLServerSocket) thriftServerSocket.getServerSocket();
      List<String> enabledProtocols = new ArrayList<String>();
      for (String protocol : sslServerSocket.getEnabledProtocols()) {
        if (sslVersionBlacklistLocal.contains(protocol.toLowerCase(Locale.ROOT))) {
          LOG.debug("Disabling SSL Protocol: " + protocol);
        } else {
          enabledProtocols.add(protocol);
        }
      }
      sslServerSocket.setEnabledProtocols(enabledProtocols.toArray(new String[0]));
      LOG.info("SSL Server Socket Enabled Protocols: "
          + Arrays.toString(sslServerSocket.getEnabledProtocols()));
    }
    return thriftServerSocket;
  }

  // retrieve delegation token for the given user
  public String getDelegationToken(String owner, String renewer, String remoteAddr)
      throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }

    try {
      String tokenStr = delegationTokenManager.getDelegationTokenWithService(owner, renewer,
          HS2_CLIENT_TOKEN, remoteAddr);
      if (tokenStr == null || tokenStr.isEmpty()) {
        throw new HiveSQLException(
            "Received empty retrieving delegation token for user " + owner, "08S01");
      }
      return tokenStr;
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error retrieving delegation token for user " + owner, "08S01", e);
    } catch (InterruptedException e) {
      throw new HiveSQLException("delegation token retrieval interrupted", "08S01", e);
    }
  }

  // cancel given delegation token
  public void cancelDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      delegationTokenManager.cancelDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error canceling delegation token " + delegationToken, "08S01", e);
    }
  }

  public void renewDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      delegationTokenManager.renewDelegationToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error renewing delegation token " + delegationToken, "08S01", e);
    }
  }

  public String verifyDelegationToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return delegationTokenManager.verifyDelegationToken(delegationToken);
    } catch (IOException e) {
      String msg =  "Error verifying delegation token " + delegationToken;
      LOG.error(msg, e);
      throw new HiveSQLException(msg, "08S01", e);
    }
  }

  public String getUserFromToken(String delegationToken) throws HiveSQLException {
    if (delegationTokenManager == null) {
      throw new HiveSQLException(
          "Delegation token only supported over kerberos authentication", "08S01");
    }
    try {
      return delegationTokenManager.getUserFromToken(delegationToken);
    } catch (IOException e) {
      throw new HiveSQLException(
          "Error extracting user from delegation token " + delegationToken, "08S01", e);
    }
  }

  public static void verifyProxyAccess(String realUser, String proxyUser, String ipAddress,
    HiveConf hiveConf) throws HiveSQLException {
    try {
      UserGroupInformation sessionUgi;
      if (UserGroupInformation.isSecurityEnabled()) {
        KerberosNameShim kerbName = ShimLoader.getHadoopShims().getKerberosNameShim(realUser);
        sessionUgi = UserGroupInformation.createProxyUser(
            kerbName.getServiceName(), UserGroupInformation.getLoginUser());
      } else {
        sessionUgi = UserGroupInformation.createRemoteUser(realUser);
      }
      if (!proxyUser.equalsIgnoreCase(realUser)) {
        ProxyUsers.refreshSuperUserGroupsConfiguration(hiveConf);
        ProxyUsers.authorize(UserGroupInformation.createProxyUser(proxyUser, sessionUgi),
            ipAddress, hiveConf);
      }
    } catch (IOException e) {
      throw new HiveSQLException(
        "Failed to validate proxy privilege of " + realUser + " for " + proxyUser, "08S01", e);
    }
  }

  public static boolean needUgiLogin(UserGroupInformation ugi, String principal, String keytab) {
    return null == ugi || !ugi.hasKerberosCredentials() || !ugi.getUserName().equals(principal) ||
      !Objects.equals(keytab, getKeytabFromUgi());
  }

  private static String getKeytabFromUgi() {
    synchronized (UserGroupInformation.class) {
      try {
        if (getKeytab != null) {
          return (String) getKeytab.invoke(UserGroupInformation.getCurrentUser());
        } else {
          return null;
        }
      } catch (Exception e) {
        LOG.debug("Fail to get keytabFile path via reflection", e);
        return null;
      }
    }
  }
}
