package org.apache.spark.ui.filters;

import jakarta.servlet.Filter;
import jakarta.servlet.FilterChain;
import jakarta.servlet.FilterConfig;
import jakarta.servlet.ServletException;
import jakarta.servlet.ServletRequest;
import jakarta.servlet.ServletResponse;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationException;
import org.apache.spark.ui.filters.hadoop_auth_copy.AuthenticationToken;

import java.io.IOException;
import java.util.Properties;

/**
 * JWTAuthFilter for Spark UI to authenticate incoming requests using the JWTAuthHandler.
 */
public class JWTAuthFilter implements Filter {

    private JWTAuthHandler authHandler;

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        Properties properties = new Properties();
        filterConfig.getInitParameterNames().asIterator().forEachRemaining(param -> {
            String value = filterConfig.getInitParameter(param);
            properties.setProperty(param, value);
        });

        authHandler = new JWTAuthHandler();
        authHandler.init(properties);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        HttpServletRequest httpRequest = (HttpServletRequest) request;
        HttpServletResponse httpResponse = (HttpServletResponse) response;

        try {
            AuthenticationToken token = authHandler.authenticate(httpRequest, httpResponse);
            if (token != null) {
                chain.doFilter(request, response);
            } else {
                httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Authentication failed");
            }
        } catch (AuthenticationException e) {
            httpResponse.sendError(HttpServletResponse.SC_FORBIDDEN, "Authentication error: " + e.getMessage());
        }
    }

    @Override
    public void destroy() {
        if (authHandler != null) {
            authHandler.destroy();
        }
    }
}
