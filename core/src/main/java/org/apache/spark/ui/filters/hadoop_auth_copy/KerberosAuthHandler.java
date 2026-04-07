package org.apache.spark.ui.filters.hadoop_auth_copy;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.apache.hadoop.security.authentication.util.KerberosName;
import org.apache.hadoop.security.authentication.util.KerberosUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.security.auth.Subject;
import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.Properties;

public class KerberosAuthHandler extends MultiMechsAuthenticationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(KerberosAuthHandler.class);

  protected GSSManager gssManager;
  protected LoginContext loginContext;
  protected String principal;
  protected String keytab;

  @Override
  public void init(Properties config) throws ServletException {
    if (loginContext != null) {
      return;
    }

    try {
      loginContext = new LoginContext("MAPR_WEBSERVER_KERBEROS");
      loginContext.login();
      javax.security.auth.login.Configuration jassConfig =
          javax.security.auth.login.Configuration.getConfiguration();
      AppConfigurationEntry[] appInfo = jassConfig.getAppConfigurationEntry("MAPR_WEBSERVER_KERBEROS");
      for (AppConfigurationEntry appConfigurationEntry : appInfo) {
        Map<String, ?> options = appConfigurationEntry.getOptions();
        principal = (String) options.get("principal");
        keytab = (String) options.get("keyTab");
      }

      Subject serverSubject = loginContext.getSubject();
      try {
        gssManager =
            Subject.doAs(
                serverSubject,
                (PrivilegedExceptionAction<GSSManager>) GSSManager::getInstance);
      } catch (PrivilegedActionException ex) {
        throw ex.getException();
      }
      LOG.info("Initialized, principal [{}] from keytab [{}]", principal, keytab);
    } catch (Exception ex) {
      KerberosUtil.checkJCEKeyStrength();
      LOG.warn(
          "Failed to obtain kerberos identity... If no Kerberos configuration was intended no further action is "
              + "needed otherwise turn on DEBUG to see full exception trace");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Full stacktrace", ex);
      }
      loginContext = null;
      throw new ServletException(ex);
    }
  }

  @Override
  public AuthenticationToken postauthenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    AuthenticationToken token = null;
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);

    if (authorization != null && authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
      authorization = authorization.substring(KerberosAuthenticator.NEGOTIATE.length()).trim();
    }
    final Base64 base64 = new Base64(0);
    final byte[] clientToken = base64.decode(authorization);
    String negotiateToken = new String(clientToken);
    if (negotiateToken.startsWith("NTLM")) {
      response.sendError(
          HttpServletResponse.SC_PRECONDITION_FAILED,
          "NTLM Authentication not supported, please try a different browser");
      LOG.info("No support for NTLM tokens is provided");
      return token;
    }

    Subject serverSubject = loginContext.getSubject();
    try {
      token =
          Subject.doAs(
              serverSubject,
              (PrivilegedExceptionAction<AuthenticationToken>)
                  () -> {
                    AuthenticationToken localToken = null;
                    GSSContext gssContext = null;
                    try {
                      gssContext = gssManager.createContext((GSSCredential) null);
                      byte[] serverToken =
                          gssContext.acceptSecContext(clientToken, 0, clientToken.length);
                      if (serverToken != null && serverToken.length > 0) {
                        String authenticate = base64.encodeToString(serverToken);
                        response.setHeader(
                            KerberosAuthenticator.WWW_AUTHENTICATE,
                            KerberosAuthenticator.NEGOTIATE + " " + authenticate);
                      }
                      if (!gssContext.isEstablished()) {
                        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
                        LOG.trace("SPNEGO in progress");
                      } else {
                        String clientPrincipal = gssContext.getSrcName().toString();
                        KerberosName kerberosName = new KerberosName(clientPrincipal);
                        String userName = kerberosName.getShortName();
                        localToken = new AuthenticationToken(userName, clientPrincipal, getType());
                        response.setStatus(HttpServletResponse.SC_OK);
                        LOG.trace("SPNEGO completed for principal [{}]", clientPrincipal);
                      }
                    } finally {
                      if (gssContext != null) {
                        gssContext.dispose();
                      }
                    }
                    return localToken;
                  });
    } catch (PrivilegedActionException ex) {
      if (ex.getException() instanceof IOException) {
        throw (IOException) ex.getException();
      } else {
        throw new AuthenticationException(ex.getException());
      }
    } catch (Exception e) {
      throw new AuthenticationException(
          "Authorization is failed, please check your config files settings", e);
    }
    return token;
  }

  @Override
  public void destroy() {
    try {
      if (loginContext != null) {
        loginContext.logout();
        loginContext = null;
      }
    } catch (LoginException ex) {
      LOG.warn(ex.getMessage(), ex);
    }
  }

  @Override
  public void addHeader(HttpServletResponse response) {
    response.addHeader(KerberosAuthenticator.WWW_AUTHENTICATE, KerberosAuthenticator.NEGOTIATE);
  }

  @Override
  public MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    if (authorization != null && authorization.startsWith(KerberosAuthenticator.NEGOTIATE)) {
      return this;
    }
    return null;
  }

  protected String getPrincipal() {
    return principal;
  }

  protected String getKeytab() {
    return keytab;
  }

  @Override
  public String getAuthorizationHeaderName() {
    return KerberosAuthenticator.NEGOTIATE;
  }
}
