package org.apache.spark.ui.filters.hadoop_auth_copy;

import org.apache.hadoop.security.authentication.client.KerberosAuthenticator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationFilter.ACTION_PARAM;

public class MultiMechsAuthenticationHandler implements AuthenticationHandler {

  private static final Logger LOG = LoggerFactory.getLogger(MultiMechsAuthenticationHandler.class);
  public static final String MTYPE = "multiauth";
  private JWTRedirectAuthenticationHandler jwt;

  public enum AuthHandlerEnum {
    BEARER {
      @Override
      public String getType() {
        return "bearer";
      }

      @Override
      public Class<? extends MultiMechsAuthenticationHandler> getMyClass() {
        return JWTAuthHandler.class;
      }

      @Override
      public int getOrder() {
        return 3;
      }
    },
    BASIC {
      @Override
      public String getType() {
        return "basic";
      }

      @Override
      public Class<? extends MultiMechsAuthenticationHandler> getMyClass() {
        return BasicAuthHandler.class;
      }

      @Override
      public int getOrder() {
        return 2;
      }
    },
    KERBEROS {
      @Override
      public String getType() {
        return "kerberos";
      }

      @Override
      public Class<? extends MultiMechsAuthenticationHandler> getMyClass() {
        return KerberosAuthHandler.class;
      }

      @Override
      public int getOrder() {
        return 1;
      }
    },
    MAPRAUTH {
      @Override
      public String getType() {
        return "maprauth";
      }

      @Override
      @SuppressWarnings("unchecked")
      public Class<? extends MultiMechsAuthenticationHandler> getMyClass() {
        try {
          Class<?> klass =
              Thread.currentThread()
                  .getContextClassLoader()
                  .loadClass("com.mapr.security.maprauth.MaprAuthenticationHandler");
          return (Class<? extends AbstractMaprAuthenticationHandler>) klass;
        } catch (ClassNotFoundException e) {
          return null;
        }
      }

      @Override
      public int getOrder() {
        return 0;
      }
    };

    public String getType() {
      return null;
    }

    public Class<? extends MultiMechsAuthenticationHandler> getMyClass() {
      return null;
    }

    public int getOrder() {
      return -1;
    }

    public static Class<? extends MultiMechsAuthenticationHandler> getClassFromType(String type) {
      for (AuthHandlerEnum value : AuthHandlerEnum.values()) {
        if (value.getType().equalsIgnoreCase(type)) {
          return value.getMyClass();
        }
      }
      return null;
    }

    public static AuthHandlerEnum[] getOrderedArray() {
      AuthHandlerEnum[] retList = new AuthHandlerEnum[AuthHandlerEnum.values().length];
      for (AuthHandlerEnum value : AuthHandlerEnum.values()) {
        retList[value.getOrder()] = value;
      }
      return retList;
    }
  }

  private final List<MultiMechsAuthenticationHandler> children = new ArrayList<>();

  @Override
  public void init(Properties config) throws ServletException {
    jwt = new JWTRedirectAuthenticationHandler();
    jwt.init(config);

    int i = 0;
    String kerberosDisable = String.valueOf(config.get("kerberos.disable"));
    while (true) {
      String type = config.getProperty("type" + i);
      if (type != null) {
        Class<? extends MultiMechsAuthenticationHandler> subClass =
            AuthHandlerEnum.getClassFromType(type);
        if (subClass != null) {
          try {
            MultiMechsAuthenticationHandler child = subClass.newInstance();
            if (!(kerberosDisable != null
                && kerberosDisable.equals("true")
                && child instanceof KerberosAuthHandler)) {
              child.init(config);
              children.add(child);
            }
          } catch (InstantiationException ex) {
            LOG.error(
                "Unable to instantiate Authenticator Subclass of type: {}. Will skip it",
                type,
                ex);
          } catch (IllegalAccessException ex) {
            LOG.error(
                "Unable to init AuthenticationHandler Subclass of type: {}. Will skip it",
                type,
                ex);
          } catch (Throwable t) {
            LOG.warn(
                "Unable to init AuthenticationHandler Subclass {} that is used for authentication type {}. "
                    + "Will skip it. If no {} configuration was intended no further action is needed "
                    + "otherwise turn on DEBUG to see full exception trace",
                subClass.getName(),
                type,
                type);
            if (LOG.isDebugEnabled()) {
              LOG.debug("Full stacktrace", t);
            }
          }
        } else {
          LOG.error(
              "No implementation found for type: {}. Corresponding class should be registered with AuthHandlerEnum",
              type);
        }
        i++;
      } else {
        break;
      }
    }

    if (children.isEmpty()) {
      if (i == 0) {
        for (AuthHandlerEnum value : AuthHandlerEnum.getOrderedArray()) {
          config.setProperty("type" + value.getOrder(), value.getType());
        }
        init(config);
      } else {
        throw new ServletException(
            "Can not proceeds with initializing Authenticators, since no concrete implementations "
                + "for AuthenticationHandler are found");
      }
    }
  }

  @Override
  public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    String authorization = request.getHeader(KerberosAuthenticator.AUTHORIZATION);
    if (authorization != null) {
      for (MultiMechsAuthenticationHandler child : children) {
        MultiMechsAuthenticationHandler result = child.getAuthBasedEntity(authorization);
        if (result != null) {
          return result.postauthenticate(request, response);
        }
      }
      LOG.error("Unknown Authorization header. Please check your config file settings and request headers");
      if (LOG.isDebugEnabled()) {
        LOG.debug("Authorization header: {}", authorization);
        LOG.debug(
            "Enabled authorization headers: {}",
            children.stream()
                .map(MultiMechsAuthenticationHandler::getAuthorizationHeaderName)
                .collect(Collectors.joining(", ")));
      }
      throw new AuthenticationException(
          "Unknown Authorization header. Please check that request has correct authorization "
              + "header that is supported by your config file settings");
    } else {
      if (request.getParameter(ACTION_PARAM) != null
          && request.getParameter(ACTION_PARAM).equalsIgnoreCase("processCode")) {
        return jwt.postauthenticate(request, response);
      }
      for (MultiMechsAuthenticationHandler child : children) {
        child.addHeader(response);
      }
      response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }
    return null;
  }

  protected void addHeader(HttpServletResponse response) {
    throw new UnsupportedOperationException(
        "Each child of this class has to implement 'addHeader' method");
  }

  protected AuthenticationToken postauthenticate(
      HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    throw new UnsupportedOperationException(
        "Each child of this class has to implement 'postauthenticate' method");
  }

  @Override
  public void destroy() {
    for (MultiMechsAuthenticationHandler child : children) {
      child.destroy();
    }
  }

  protected MultiMechsAuthenticationHandler getAuthBasedEntity(String authorization) {
    throw new UnsupportedOperationException(
        "Each child of this class has to implement 'getAuthBasedEntity' method");
  }

  @Override
  public String getType() {
    return MTYPE;
  }

  public String getAuthorizationHeaderName() {
    throw new UnsupportedOperationException(
        "Each child of this class has to implement 'getAuthorizationHeaderName' method");
  }

  @Override
  public boolean managementOperation(
      AuthenticationToken token, HttpServletRequest request, HttpServletResponse response)
      throws IOException, AuthenticationException {
    return true;
  }
}
