package org.apache.spark.ui.filters;

import org.apache.hadoop.security.authentication.client.AuthenticationException;
import org.apache.hadoop.security.authentication.server.AuthenticationToken;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.*;
import static org.mockito.MockitoAnnotations.openMocks;

public final class ProxyHeaderAuthenticationHandlerTest {

    private static final String DEFAULT_USERNAME_HEADER = "impersonate-user";
    private static final String USERNAME_HEADER_NAME_PARAM = "userheader";
    @Mock
    private HttpServletRequest mockedRequest;
    @Mock
    private HttpServletResponse mockedResponse;
    @Mock
    private Properties handlerProperties;

    private AutoCloseable mocks;

    @Before
    public void init() {
        mocks = openMocks(this);
    }

    @After
    public void tearDown() throws Exception {
        mocks.close();
    }

    @Test
    public void shouldAuthenticateIfDefaultUsernameHeaderIsUsed() throws ServletException, AuthenticationException, IOException {
        when(mockedRequest.getHeader(DEFAULT_USERNAME_HEADER)).thenReturn("foo");

        ProxyAuthenticationHandler headerAuthenticationHandler = new ProxyAuthenticationHandler();
        headerAuthenticationHandler.init(new Properties());
        AuthenticationToken token = headerAuthenticationHandler.authenticate(mockedRequest, mockedResponse);

        assertEquals("foo", token.getUserName());
        verify(mockedResponse).setStatus(HttpServletResponse.SC_OK);
    }

    @Test
    public void shouldAuthenticateIfCustomUsernameHeaderIsUsed() throws ServletException, AuthenticationException, IOException {
        final String customHeaderName = "customUserHeader";
        when(handlerProperties.getProperty(USERNAME_HEADER_NAME_PARAM)).thenReturn(customHeaderName);
        when(mockedRequest.getHeader(customHeaderName)).thenReturn("bar");

        ProxyAuthenticationHandler headerAuthenticationHandler = new ProxyAuthenticationHandler();
        headerAuthenticationHandler.init(handlerProperties);
        AuthenticationToken token = headerAuthenticationHandler.authenticate(mockedRequest, mockedResponse);

        assertEquals("bar", token.getUserName());
        verify(mockedResponse).setStatus(HttpServletResponse.SC_OK);
    }

    @Test
    public void shouldFailToAuthenticateIfUserHeaderIsNull() throws ServletException, AuthenticationException, IOException {
        when(mockedRequest.getHeader(DEFAULT_USERNAME_HEADER)).thenReturn(null);

        ProxyAuthenticationHandler handler = new ProxyAuthenticationHandler();
        handler.init(handlerProperties);
        AuthenticationToken token = handler.authenticate(mockedRequest, mockedResponse);

        assertNull(token);
        verify(mockedResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

    @Test
    public void shouldFailToAuthenticateIfUserHeaderIsEmpty() throws ServletException, AuthenticationException, IOException {
        when(mockedRequest.getHeader(DEFAULT_USERNAME_HEADER)).thenReturn("");

        ProxyAuthenticationHandler handler = new ProxyAuthenticationHandler();
        handler.init(handlerProperties);
        AuthenticationToken token = handler.authenticate(mockedRequest, mockedResponse);

        assertNull(token);
        verify(mockedResponse).setStatus(HttpServletResponse.SC_UNAUTHORIZED);
    }

}
