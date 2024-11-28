package org.apache.spark.ui.filters;

import com.auth0.jwk.*;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Verification;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationHandler;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.*;

import static org.apache.spark.ui.filters.HttpConstants.AUTHORIZATION_HEADER;

/**
 * Primitive JWT authentication handler designed to authenticate requests with Bearer scheme
 * Configuration parameters:
 * - user.identity.claim - JWT claim to be used to get username from token
 */
public final class JWTAuthHandler implements AuthenticationHandler {

    private static Logger logger = LoggerFactory.getLogger(JWTAuthHandler.class);
    private static final String IDENTITY_CLAIM_PARAM = "jwt.identity.claim";
    private static final String JWK_PROVIDER_URL_PARAM = "jwk.provider.url";
    private static final String JWT_ISSUERS_PARAM = "jwt.issuers";
    private static final String JWT_AUDIENCE_PARAM = "jwt.audiences";

    private final String type = "jwt-bearer";
    private List<JWTConfig> jwtConfigList;

    @Override
    public String getType() {
        return type;
    }

    @Override
    public void init(Properties config) throws ServletException {
        logger.info("Initializing JWTAuthHandler");

        jwtConfigList = new ArrayList<>();

        for (String key: config.stringPropertyNames()) {
            if (key.startsWith(JWK_PROVIDER_URL_PARAM)) {
                String providerNumber = key.substring(JWK_PROVIDER_URL_PARAM.length());
                String jwkProviderURL = config.getProperty(key);

                String issuersKey = JWT_ISSUERS_PARAM + providerNumber;
                String audKey = JWT_AUDIENCE_PARAM + providerNumber;
                String identityClaimKey = IDENTITY_CLAIM_PARAM + providerNumber;

                String jwtIssuers = config.getProperty(issuersKey);
                String jwtAud = config.getProperty(audKey);
                String userIdentityClaim = config.getProperty(identityClaimKey);

                if (jwkProviderURL != null && jwtIssuers != null && userIdentityClaim != null) {
                    try {
                        JwkProvider jwkProvider = new JwkProviderBuilder(new URL(jwkProviderURL)).build();
                        List<String> issuersList = List.of(jwtIssuers.split(","));
                        List<String> audList = jwtAud != null ? List.of(jwtAud.split(",")) : List.of();

                        jwtConfigList.add(new JWTConfig(jwkProvider, issuersList, audList, userIdentityClaim));

                        logger.info("Added provider: {} with issuers: {} and audiences: {}, and identity claim: {}",
                                jwkProviderURL, issuersList, audList, userIdentityClaim);
                    } catch (MalformedURLException e) {
                        throw new RuntimeException("JwtProviderURL is not set or is invalid");
                    }
                } else {
                    logger.error("Missing issuers or audience for provider: {}", jwkProviderURL);
                }
            }
        }
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

        for (JWTConfig jwtConfig: jwtConfigList) {
            if (jwtConfig.issuers.contains(decodedJWT.getIssuer())) {
                AuthenticationToken token = verifyAndAuthenticate(decodedJWT, jwtConfig);
                if (token != null) {
                    return token;
                }
            }
        }

        logger.error("No valid JWKS provider could verify the token");
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return null;
    }

    private AuthenticationToken verifyAndAuthenticate(DecodedJWT decodedJWT, JWTConfig jwtConfig) {
        try {
            RSAPublicKey publicKey = (RSAPublicKey) jwtConfig.jwkProvider.get(decodedJWT.getKeyId()).getPublicKey();
            Algorithm algorithm = Algorithm.RSA256(publicKey, null);

            Verification verifierBuilder = JWT.require(algorithm)
                    .withIssuer(jwtConfig.issuers.toArray(new String[0]));

            List<String> audiences = jwtConfig.audiences;
            if (audiences != null && !audiences.isEmpty()) {
                verifierBuilder.withAudience(audiences.toArray(new String[0]));
            }

            JWTVerifier verifier = verifierBuilder.build();
            verifier.verify(decodedJWT);

            String username = decodedJWT.getClaim(jwtConfig.identityClaim).asString();
            if (username != null && !username.trim().isEmpty()) {
                logger.info("Authenticated user: {}", username);
                return new AuthenticationToken(username, username, "jwt-bearer");
            } else {
                logger.error("Username claim '{}' not found in token", jwtConfig.identityClaim);
                return null;
            }

        } catch (Exception ex) {
            logger.error("Token verification failed", ex);
            return null;
        }
    }

    private static class JWTConfig {
        private final JwkProvider jwkProvider;
        private final List<String> issuers;
        private final List<String> audiences;
        private final String identityClaim;

        public JWTConfig(JwkProvider jwkProvider, List<String> issuers, List<String> audiences, String identityClaim) {
            this.jwkProvider = jwkProvider;
            this.issuers = issuers;
            this.audiences = audiences;
            this.identityClaim = identityClaim;
        }
    }

}
