package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletRequestWrapper;

/**
 * Wrap {@link HttpServletRequest} so that it honors the "X-Real-IP" header.
 */
public class ClientIPFilter implements Filter {

    /** Header name for X-Client-IP. */
    private static final String X_CLIENT_IP = "X-Client-IP";

    /** {@inheritDoc} */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do nothing
    }

    /**
     * Wrap {@link HttpServletRequest} so that it honors the "X-Real-IP" header.
     *
     * @param request {@inheritDoc}
     * @param response {@inheritDoc}
     * @param chain {@inheritDoc}
     * @throws IOException {@inheritDoc}
     * @throws ServletException {@inheritDoc}
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            chain.doFilter(new RealIPHttpRequestWrapper((HttpServletRequest) request), response);
        } else {
            chain.doFilter(request, response);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // Do nothing
    }


    /** Wrapping the request and implementing lookup of remoteAddr with x-client-ip header. */
    private static final class RealIPHttpRequestWrapper extends HttpServletRequestWrapper {

        /** Constructor. */
        private RealIPHttpRequestWrapper(HttpServletRequest request) {
            super(request);
        }

        /** {@inheritDoc} */
        @Override
        public String getRemoteAddr() {
            String realIP = getHeader(X_CLIENT_IP);
            return realIP != null ? realIP : super.getRemoteAddr();
        }

        /** {@inheritDoc} */
        @Override
        public String getRemoteHost() {
            try {
                return InetAddress.getByName(getRemoteAddr()).getHostName();
            } catch (UnknownHostException e) {
                return getRemoteAddr();
            }
        }
    }
}
