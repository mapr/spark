package org.apache.spark.ui.filters;

import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CustomHeadersFilter implements Filter {

    private static final Logger LOG = LoggerFactory.getLogger(CustomHeadersFilter.class);

    private static final String UI_CUSTOM_HEADERS_FILE_PATH = "spark.ui.headers";

    private static final Properties CUSTOM_HEADER_PROPERTIES = new Properties();

    private static final Properties DEFAULT_HEADER_PROPERTIES = new Properties();

    private static File customHeaderFile;

    private static boolean customHeadersEnabled;

    @Override
    public void init(FilterConfig filterConfig) {
        if (UserGroupInformation.isSecurityEnabled()) {
            initDefaultHeaderProperties();
        }

        customHeadersEnabled = filterConfig.getInitParameter(UI_CUSTOM_HEADERS_FILE_PATH) != null;
        if (customHeadersEnabled) {
            customHeaderFile = new File(filterConfig.getInitParameter(UI_CUSTOM_HEADERS_FILE_PATH));
            initCustomHeaderProperties(customHeaderFile);
        }
    }

    private void initDefaultHeaderProperties() {
        DEFAULT_HEADER_PROPERTIES.put("Strict-Transport-Security", "max-age=31536000;includeSubDomains");
        DEFAULT_HEADER_PROPERTIES.put("Content-Security-Policy", "default-src https:;" +
                "script-src 'self' 'unsafe-inline';" +
                "style-src 'self' 'unsafe-inline';" +
                "font-src data:");
        DEFAULT_HEADER_PROPERTIES.put("X-Content-Type-Options", "nosniff");
        DEFAULT_HEADER_PROPERTIES.put("X-XSS-Protection", "1; mode=block");
    }

    private void initCustomHeaderProperties(File headerFile) {
        try {
            CUSTOM_HEADER_PROPERTIES.loadFromXML(new FileInputStream(headerFile));
        } catch (IOException e) {
            LOG.error(e.toString());
        }
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        HttpServletResponse httpServletResponse = (HttpServletResponse) response;

        DEFAULT_HEADER_PROPERTIES.forEach((k, v) -> httpServletResponse.addHeader((String) k, (String) v));

        if (customHeadersEnabled) {
            if (customHeaderFile.exists()) {
                CUSTOM_HEADER_PROPERTIES.forEach((k, v) -> httpServletResponse.setHeader((String) k, (String) v));
            } else {
                String message = "Jetty headers configuration " + customHeaderFile.getAbsolutePath() +
                        " configured with spark.ui.headers in spark-defaults.conf" +
                        " was not found";
                httpServletResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        //noting to do
    }
}
