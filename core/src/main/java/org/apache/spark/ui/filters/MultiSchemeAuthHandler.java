package org.apache.spark.ui.filters;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.*;

import static org.apache.spark.ui.filters.HttpConstants.*;

/**
 * Copy of org.apache.hadoop.security.authentication.server.MultiSchemeAuthenticationHandler
 * Original handler can't be used because it doesn't support 'Bearer' scheme, and custom handler util can't be injected
 *
 * Differences from original class:
 * - added support for 'Bearer' scheme
 * - fixed NPE in 'authenticate' method for case if internal handler returned null token
 */
public final class MultiSchemeAuthHandler implements AuthenticationHandler {
    private static Logger logger = LoggerFactory
            .getLogger(MultiSchemeAuthHandler.class);
    public static final String SCHEMES_PROPERTY =
            "multi-scheme-auth-handler.schemes";
    public static final String AUTH_HANDLER_PROPERTY =
            "multi-scheme-auth-handler.schemes.%s.handler";
    private static final Splitter STR_SPLITTER = Splitter.on(',').trimResults()
            .omitEmptyStrings();

    private final Map<String, AuthenticationHandler> schemeToAuthHandlerMapping =
            new HashMap<>();
    private final Collection<String> types = new HashSet<>();
    private final String authType;

    /**
     * Constant that identifies the authentication mechanism.
     */
    public static final String TYPE = "multi-scheme";

    public MultiSchemeAuthHandler() {
        this(TYPE);
    }

    public MultiSchemeAuthHandler(String authType) {
        this.authType = authType;
    }

    @Override
    public String getType() {
        return authType;
    }

    @Override
    public void init(Properties config) throws ServletException {
        // Useful for debugging purpose.
        for (Map.Entry prop : config.entrySet()) {
            logger.info("{} : {}", prop.getKey(), prop.getValue());
        }

        this.types.clear();

        String schemesProperty =
                Preconditions.checkNotNull(config.getProperty(SCHEMES_PROPERTY),
                        "%s system property is not specified.", SCHEMES_PROPERTY);
        for (String scheme : STR_SPLITTER.split(schemesProperty)) {
            scheme = checkAuthScheme(scheme);
            if (schemeToAuthHandlerMapping.containsKey(scheme)) {
                throw new IllegalArgumentException("Handler is already specified for "
                        + scheme + " authentication scheme.");
            }

            String authHandlerPropName =
                    String.format(AUTH_HANDLER_PROPERTY, scheme).toLowerCase();
            String authHandlerClassName = config.getProperty(authHandlerPropName);
            Preconditions.checkNotNull(authHandlerClassName,
                    "No auth handler configured for scheme %s.", scheme);

            AuthenticationHandler handler =
                    initializeAuthHandler(authHandlerClassName, config);
            schemeToAuthHandlerMapping.put(scheme, handler);
            types.add(handler.getType());
        }
        logger.info("Successfully initialized MultiSchemeAuthenticationHandler");
    }

    @Override
    public void destroy() {
        for (AuthenticationHandler handler : schemeToAuthHandlerMapping.values()) {
            handler.destroy();
        }
    }

    @Override
    public boolean managementOperation(AuthenticationToken token,
                                       HttpServletRequest request, HttpServletResponse response)
            throws IOException, AuthenticationException {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request,
                                            HttpServletResponse response)
            throws IOException, AuthenticationException {
        String authorization =
                request.getHeader(HttpConstants.AUTHORIZATION_HEADER);
        if (authorization != null) {
            for (Map.Entry<String, AuthenticationHandler> entry :
                    schemeToAuthHandlerMapping.entrySet()) {
                if (matchAuthScheme(entry.getKey(), authorization)) {
                    AuthenticationToken token =
                            entry.getValue().authenticate(request, response);
                    if (token != null) {
                        logger.trace("Token generated with type {}", token.getType());
                    } else {
                        logger.trace("Generated null token");
                    }
                    return token;
                }
            }
        }

        // Handle the case when (authorization == null) or an invalid authorization
        // header (e.g. a header value without the scheme name).
        response.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
        for (String scheme : schemeToAuthHandlerMapping.keySet()) {
            response.addHeader(HttpConstants.WWW_AUTHENTICATE_HEADER, scheme);
        }

        return null;
    }

    private AuthenticationHandler initializeAuthHandler(
            String authHandlerClassName, Properties config) throws ServletException {
        try {
            Preconditions.checkNotNull(authHandlerClassName);
            logger.debug("Initializing Authentication handler of type {}", authHandlerClassName);
            Class<?> klass =
                    Thread.currentThread().getContextClassLoader()
                            .loadClass(authHandlerClassName);
            AuthenticationHandler authHandler =
                    (AuthenticationHandler) klass.newInstance();
            authHandler.init(config);
            logger.info("Successfully initialized Authentication handler of type {}", authHandlerClassName);
            return authHandler;
        } catch (ClassNotFoundException | InstantiationException
                 | IllegalAccessException ex) {
            logger.error("Failed to initialize authentication handler "
                    + authHandlerClassName, ex);
            throw new ServletException(ex);
        }
    }

    private static String checkAuthScheme(String scheme) {
        if (BASIC.equalsIgnoreCase(scheme)) {
            return BASIC;
        } else if (NEGOTIATE.equalsIgnoreCase(scheme)) {
            return NEGOTIATE;
        } else if (DIGEST.equalsIgnoreCase(scheme)) {
            return DIGEST;
        } else if (BEARER.equalsIgnoreCase(scheme)) {
            return BEARER;
        }
        throw new IllegalArgumentException(String.format(
                "Unsupported HTTP authentication scheme %s ."
                        + " Supported schemes are [%s, %s, %s, %s]", scheme, BASIC, NEGOTIATE,
                DIGEST, BEARER));
    }

    private static boolean matchAuthScheme(String scheme, String auth) {
        scheme = Preconditions.checkNotNull(scheme).trim();
        auth = Preconditions.checkNotNull(auth).trim();
        return auth.regionMatches(true, 0, scheme, 0, scheme.length());
    }
}
