package org.apache.spark.ui.filters;

import com.auth0.jwt.JWT;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
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
 * Primitive JWT authentication handler designed to authenticate requests with Bearer scheme
 * Configuration parameters:
 * - user.identity.claim - JWT claim to be used to get username from token
 */
public final class JWTAuthHandler implements AuthenticationHandler {

    private static Logger logger = LoggerFactory.getLogger(JWTAuthHandler.class);
    private static final String IDENTITY_CLAIM_PARAM = "user.identity.claim";

    private final String type = "jwt-bearer";
    private String userIdentityClaim;

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void init(Properties config) throws ServletException {
        logger.info("Initializing JWTAuthHandler");
        this.userIdentityClaim = config.getProperty(IDENTITY_CLAIM_PARAM, "preferred-username");
        logger.info("Will use {} as identity claim", this.userIdentityClaim);
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

        if (authHeader == null || !authHeader.toLowerCase().startsWith("bearer")) {
            logger.error("Unexpected or empty auth header: {}", authHeader);
            throw new AuthenticationException("Unsupported auth scheme in header: " + authHeader);
        }

        String[] splitHeader = authHeader.split(" ");
        if (splitHeader.length != 2) {
            logger.error("Too many parts in auth header (expected exactly 2, but was {}): {}", splitHeader.length, authHeader);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        String jwtString = splitHeader[1];
        DecodedJWT decodedJWT;

        try {
            decodedJWT = JWT.decode(jwtString);
        } catch (JWTDecodeException e) {
            logger.error("Failed to decode given JWT", e);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
            return null;
        }

        String username = decodedJWT.getClaim(userIdentityClaim).asString();
        if (username != null && !"".equals(username.trim())) {
            logger.info("Authenticating user {}", username);
            token = new AuthenticationToken(username, username, type);
        } else {
            logger.info("Failed to retrieve username from token by claim {}", this.userIdentityClaim);
            response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        }

        return token;
    }
}
