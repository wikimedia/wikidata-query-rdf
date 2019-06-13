package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;

import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.FilterConfig;
import javax.servlet.ServletException;
import javax.servlet.ServletRequest;
import javax.servlet.ServletResponse;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * This filter bans any request that does not have proper User-Agent header.
 * For now, it means string longer than 5 characters.
 */
public class RealAgentFilter implements Filter {
    @Override
    public void init(FilterConfig filterConfig) throws ServletException {
        // Do nothing
    }

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain) throws IOException, ServletException {
        if (request instanceof HttpServletRequest) {
            String userAgent = ((HttpServletRequest)request).getHeader("User-Agent");
            if (userAgent == null || userAgent.length() < 5) {
                ((HttpServletResponse)response).sendError(403, "Please identify your user agent, see https://meta.wikimedia.org/wiki/User-Agent_policy");
                return;
            }
        }
        chain.doFilter(request, response);
    }

    @Override
    public void destroy() {
        // Do nothing
    }
}
