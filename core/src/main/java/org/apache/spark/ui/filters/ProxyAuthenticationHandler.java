package org.apache.spark.ui.filters;

import org.apache.commons.lang3.StringUtils;
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

public final class ProxyAuthenticationHandler implements AuthenticationHandler {

    private static final Logger LOG = LoggerFactory.getLogger(ProxyAuthenticationHandler.class);
    private static final String TYPE = "proxyauth";
    private static final String PROXY_USER_HEADER_PARAM = "userheader";

    private String userNameHeaderName = "impersonate-user";

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public void init(Properties properties) throws ServletException {
        String configuredUserNameHeader = properties.getProperty(PROXY_USER_HEADER_PARAM);
        if (configuredUserNameHeader != null) {
            this.userNameHeaderName = configuredUserNameHeader;
        }
        LOG.debug("Using {} as username header name", this.userNameHeaderName);
    }

    @Override
    public void destroy() {
        // no resources allocated by this handler, therefore nothing to do
    }

    @Override
    public boolean managementOperation(AuthenticationToken authenticationToken, HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, AuthenticationException {
        return true;
    }

    @Override
    public AuthenticationToken authenticate(HttpServletRequest httpServletRequest, HttpServletResponse httpServletResponse) throws IOException, AuthenticationException {
        AuthenticationToken token = null;

        String userNameHeaderValue = httpServletRequest.getHeader(userNameHeaderName);

        if (userNameHeaderValue == null || StringUtils.isBlank(userNameHeaderValue)) {
            httpServletResponse.setStatus(HttpServletResponse.SC_UNAUTHORIZED);
            LOG.warn("Empty username in {} header", userNameHeaderName);
        } else {
            httpServletResponse.setStatus(HttpServletResponse.SC_OK);
            token = new AuthenticationToken(userNameHeaderValue, userNameHeaderValue, TYPE);
            LOG.debug("Authenticated as {}", userNameHeaderValue);
        }

        return token;
    }
}
