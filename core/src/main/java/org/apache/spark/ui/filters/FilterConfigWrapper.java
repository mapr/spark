package org.apache.spark.ui.filters;

import org.apache.commons.collections.iterators.IteratorEnumeration;

import javax.servlet.FilterConfig;
import javax.servlet.ServletContext;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;

public class FilterConfigWrapper implements FilterConfig {

  private Map<String, String> conf = new HashMap<>();
  private ServletContext servletContext;
  private String filterName;

  @Override
  public String getFilterName() {
    return filterName;
  }

  public void setFilterName(String filterName) {
    this.filterName = filterName;
  }

  @Override
  public ServletContext getServletContext() {
    return servletContext;
  }

  public void setServletContext(ServletContext servletContext) {
    this.servletContext = servletContext;
  }

  @Override
  public String getInitParameter(String s) {
    return conf.get(s);
  }

  public void setInitParameter(String key, String value) {
    conf.put(key, value);
  }

  @Override
  public Enumeration<String> getInitParameterNames() {
    return new IteratorEnumeration(conf.keySet().iterator());
  }
}
