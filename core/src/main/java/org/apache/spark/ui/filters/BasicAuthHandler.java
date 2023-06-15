package org.apache.spark.ui.filters;

import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

import static org.apache.spark.ui.filters.HttpConstants.AUTHORIZATION_HEADER;

/**
 * Copy of com.mapr.security.maprauth.BasicAuthHandler
 * The original auth handler can't be used because of its coupling with mapr MultiMechsAuthenticationHandler
 */
public final class BasicAuthHandler implements AuthenticationHandler {

    private static Logger logger = LoggerFactory.getLogger(BasicAuthHandler.class);
    private static final String TYPE = "Basic";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties config) throws ServletException {
        logger.info("Initialized BasicAuthHandler");
    }

    @Override
    public void destroy() {

    }

    @Override
    public boolean managementOperation(AuthenticationToken token, HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest request, HttpServletResponse response) throws IOException, AuthenticationException {
        AuthenticationToken token = null;
        String authHeader = request.getHeader(AUTHORIZATION_HEADER);

        if (authHeader == null || !authHeader.toLowerCase().startsWith("basic")) {
            logger.error("Unexpected or empty auth header: {}", authHeader);
            throw new AuthenticationException("Unsupported auth scheme in header: " + authHeader);
        }

        String[] splitHeader = authHeader.split(" ");
        if (splitHeader.length != 2) {
            logger.error("Too many parts in auth header (expected exactly 2, but was {}): {}", splitHeader.length, authHeader);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        String encodedCreds = splitHeader[1];
        String[] decodedCreds = new String(Base64.decodeBase64(encodedCreds)).split(":");

        if (JPAMMaprPasswordAuthenticator.authenticate(decodedCreds[0], decodedCreds[1])) {
            logger.info("Successfully authenticated user {}", decodedCreds[0]);
            response.setStatus(HttpServletResponse.SC_OK);
            token = new AuthenticationToken(decodedCreds[0], decodedCreds[0], TYPE);
        } else {
            logger.info("Failed to authenticate user {}", decodedCreds[0]);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        }

        return token;
    }
}
