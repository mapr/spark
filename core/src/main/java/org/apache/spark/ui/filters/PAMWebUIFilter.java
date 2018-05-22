package org.apache.spark.ui.filters;

import com.mapr.login.PasswordAuthentication;
import com.sun.jersey.core.util.Base64;
import org.eclipse.jetty.http.HttpHeader;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.StringTokenizer;

public class PAMWebUIFilter implements Filter {

    private final String REALM = "Basic realm=\"Spark Realm\"";

    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
    }

    @Override
    public void doFilter(ServletRequest servletRequest, ServletResponse servletResponse, FilterChain filterChain) throws IOException, ServletException {
        HttpServletRequest request = (HttpServletRequest) servletRequest;
        HttpServletResponse response = (HttpServletResponse) servletResponse;

        String authHeader = request.getHeader("Authorization");
        if (authHeader != null) {
            StringTokenizer st = new StringTokenizer(authHeader);
            if (st.hasMoreTokens()) {
                String basic = st.nextToken();
                if (basic.equalsIgnoreCase("Basic")) {
                    try {
                        String credentials = new String(Base64.decode(st.nextToken()), "UTF-8");
                        int pos = credentials.indexOf(":");
                        if (pos != -1) {
                            String username = credentials.substring(0, pos).trim();
                            String password = credentials.substring(pos + 1).trim();

                            if (!PasswordAuthentication.authenticate(username, password)) {
                                unauthorized(response, "Unauthorized:" +
                                        this.getClass().getCanonicalName());
                            }

                            HttpServletRequest requestWrapper = new RemoteUserWrapper(username, request);
                            filterChain.doFilter(requestWrapper, servletResponse);
                        } else {
                            unauthorized(response, "Unauthorized:" +
                                    this.getClass().getCanonicalName());
                        }
                    } catch (UnsupportedEncodingException e) {
                        throw new Error("Couldn't retrieve authorization information", e);
                    }
                }
            }
        } else {
            unauthorized(response, "Unauthorized:" + this.getClass().getCanonicalName());
        }
    }

    @Override
    public void destroy() {
    }

    private void unauthorized(HttpServletResponse response, String message) throws IOException {
        response.setHeader(HttpHeader.WWW_AUTHENTICATE.asString(), REALM);
        response.sendError(401, message);
    }

}
