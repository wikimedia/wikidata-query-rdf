package org.wikidata.query.rdf.blazegraph.filters;

import org.slf4j.MDC;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import java.io.Closeable;
import java.io.IOException;

/**
 * Add user-agent and IP address to the logging MDC.
 *
 * The Mapped Diagnostic Context (MDC) is a way to expose context to the
 * logging framework, which can then be printed with each log message.
 *
 * See <a href="https://logback.qos.ch/manual/mdc.html">the logback
 * documentation</a> for details on MDC.
 */
public class LoggingFilter implements Filter {

    /** {@inheritDoc} */
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do nothing
    }

    /**
     * Add user-agent and IP address to the logging MDC.
     *
     * @param request
     * @param response
     * @param chain
     * @throws IOException
     * @throws ServletException
     */
    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        String remoteAddr = request.getRemoteAddr();
        String userAgent = null;
        if (request instanceof HttpServletRequest) {
            HttpServletRequest httpRequest = (HttpServletRequest) request;
            userAgent = httpRequest.getHeader("User-Agent");
        }

        try (
                Closeable mdcIPAddress = MDC.putCloseable("IPAddress", remoteAddr);
                Closeable mdcUserAgent = MDC.putCloseable("UserAgent", userAgent)
        ) {
            chain.doFilter(request, response);
        }
    }

    /** {@inheritDoc} */
    @Override
    public void destroy() {
        // do nothing
    }
}
