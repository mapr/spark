package org.apache.spark.ui.filters;

import com.auth0.jwk.*;
import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTVerifier;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTDecodeException;
import com.auth0.jwt.interfaces.DecodedJWT;
import com.auth0.jwt.interfaces.Verification;
import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.security.KeyFactory;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.X509EncodedKeySpec;
import java.util.*;

import static org.apache.spark.ui.filters.HttpConstants.AUTHORIZATION_HEADER;

/**
 * JWT authentication handler designed to authenticate requests with Bearer scheme.
 * Supports both JWKS URL and local PEM files for token verification.
 */
public final class JWTAuthHandler extends MultiSchemeAuthHandler {

    private static Logger logger = LoggerFactory.getLogger(JWTAuthHandler.class);

    private static final String JWK_PROVIDER_PARAM_PREFIX = "jwk.";
    private static final String JWT_PARAM_PREFIX = "jwt.";
    private static final String PROVIDER_URL_SUFFIX = ".provider.source";
    private static final String ISSUER_SUFFIX = ".issuer";
    private static final String AUDIENCES_SUFFIX = ".audiences";
    private static final String IDENTITY_CLAIM_SUFFIX = ".identity.claim";

    private List<JWTConfig> jwtConfigList;

    @Override
    public void init(Properties config) throws ServletException {
        logger.info("Initializing JWTAuthHandler");

        jwtConfigList = new ArrayList<>();

        for (String key: config.stringPropertyNames()) {

            if (key.startsWith(JWK_PROVIDER_PARAM_PREFIX) && key.endsWith(PROVIDER_URL_SUFFIX)) {
                String providerName = key.substring(
                        JWK_PROVIDER_PARAM_PREFIX.length(),
                        key.length() - PROVIDER_URL_SUFFIX.length()
                );

                String source = config.getProperty(key);
                String issuerKey = JWT_PARAM_PREFIX + providerName + ISSUER_SUFFIX;
                String audKey = JWT_PARAM_PREFIX + providerName + AUDIENCES_SUFFIX;
                String identityClaimKey = JWT_PARAM_PREFIX + providerName + IDENTITY_CLAIM_SUFFIX;

                String jwtIssuer = config.getProperty(issuerKey);
                String jwtAud = config.getProperty(audKey);
                String userIdentityClaim = config.getProperty(identityClaimKey);

                if (source != null && jwtIssuer != null && userIdentityClaim != null) {
                    List<String> audList = jwtAud != null ? List.of(jwtAud.split(",")) : List.of();

                    jwtConfigList.add(new JWTConfig(source, jwtIssuer, audList, userIdentityClaim));
                    logger.info("Added provider '{}' with issuer: {}, audiences: {}, and identity claim: {}",
                            providerName, jwtIssuer, audList, userIdentityClaim);
                } else {
                    logger.error("Missing issuer or claim for provider '{}'", providerName);
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
            if (jwtConfig.issuer.equals(decodedJWT.getIssuer())) {
                AuthenticationToken token = verifyAndAuthenticate(decodedJWT, jwtConfig);
                if (token != null) {
                    return token;
                }
            }
        }

        logger.error("No valid JWKS could verify the token");
        response.setStatus(HttpServletResponse.SC_FORBIDDEN);
        return null;
    }

    private AuthenticationToken verifyAndAuthenticate(DecodedJWT decodedJWT, JWTConfig jwtConfig) {
        try {
            RSAPublicKey publicKey = jwtConfig.getPublicKey(decodedJWT.getKeyId());
            Algorithm algorithm = Algorithm.RSA256(publicKey, null);

            Verification verifierBuilder = JWT.require(algorithm)
                    .withIssuer(jwtConfig.issuer);

            List<String> audiences = jwtConfig.audiences;
            if (audiences != null && !audiences.isEmpty()) {
                verifierBuilder.withAudience(audiences.toArray(new String[0]));
            }

            JWTVerifier verifier = verifierBuilder.build();
            verifier.verify(decodedJWT);

            String username = decodedJWT.getClaim(jwtConfig.identityClaim).asString();
            if (username != null && !username.trim().isEmpty()) {
                logger.info("Authenticated user: {}", username);
                return new AuthenticationToken(username, username, getType());
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
        private final String issuer;
        private final List<String> audiences;
        private final String identityClaim;
        private final JwkProvider jwkProvider;
        private RSAPublicKey publicKey;

        public JWTConfig(String source, String issuer, List<String> audiences, String identityClaim) {
            this.issuer = issuer;
            this.audiences = audiences;
            this.identityClaim = identityClaim;

            if (isValidURL(source)) {
                try {
                    this.jwkProvider = new JwkProviderBuilder(new URL(source))
                            .cached(true)
                            .build();
                } catch (MalformedURLException e) {
                    throw new RuntimeException("Invalid JWKS URL: " + source, e);
                }
            } else {
                try {
                    this.jwkProvider = null;
                    this.publicKey = loadPublicKeyFromFile(source);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }

        public RSAPublicKey getPublicKey(String keyId) {
            if (jwkProvider != null) {
                try {
                    Jwk jwk = jwkProvider.get(keyId);
                    return (RSAPublicKey) jwk.getPublicKey();
                } catch (JwkException e) {
                    throw new RuntimeException("Failed to get public key from JWKS provider", e);
                }
            }
            return publicKey;
        }

        private RSAPublicKey loadPublicKeyFromFile(String filePath) throws IOException {
            try (PemReader pemReader = new PemReader(new FileReader(filePath))) {
                PemObject pemObject = pemReader.readPemObject();
                byte[] keyBytes = pemObject.getContent();
                KeyFactory keyFactory = KeyFactory.getInstance("RSA");
                X509EncodedKeySpec keySpec = new X509EncodedKeySpec(keyBytes);
                return (RSAPublicKey) keyFactory.generatePublic(keySpec);
            } catch (Exception e) {
                throw new IOException("Failed to load public key from file", e);
            }
        }

        private boolean isValidURL(String source) {
            try {
                URL url = new URL(source);
                String protocol = url.getProtocol();
                return protocol.equalsIgnoreCase("http") ||
                        protocol.equalsIgnoreCase("https") ||
                        protocol.equalsIgnoreCase("ftp");
            } catch (MalformedURLException e) {
                return false;
            }
        }
    }

}
