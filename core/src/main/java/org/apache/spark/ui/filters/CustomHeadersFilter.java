package org.apache.spark.ui.filters;

import org.apache.spark.SparkConf;
import org.apache.spark.util.Utils;
import scala.collection.JavaConversions;

import javax.servlet.*;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class CustomHeadersFilter implements Filter {
    private SparkConf sparkConf;

    private final static String UI_SPARK_HEADERS_FILE_PATH_CONFIG = "spark.ui.headers.file";

    @Override
    public void init(FilterConfig filterConfig) {
        String confFileName = Utils.getDefaultPropertiesFile(JavaConversions.mapAsScalaMap(System.getenv()));
        sparkConf = new SparkConf();
        Utils.loadDefaultSparkProperties(sparkConf, confFileName);
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (sparkConf.contains(UI_SPARK_HEADERS_FILE_PATH_CONFIG)) {
            HttpServletResponse httpServletResponse = (HttpServletResponse) response;
            File headerFile = new File(sparkConf.get(UI_SPARK_HEADERS_FILE_PATH_CONFIG));
            if (headerFile.exists()) {
                Properties headerProperties = new Properties();
                headerProperties.loadFromXML(new FileInputStream(headerFile));
                headerProperties.forEach((k, v) -> httpServletResponse.addHeader((String) k, (String) v));
            } else {
                String message = "Jetty headers configuration " + headerFile.getAbsolutePath() +
                        " configured with spark.ui.headers in spark-defaults.conf" +
                        " was not found";
                httpServletResponse.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, message);
            }
        }

        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
    }
}
