package org.wikidata.query.rdf.blazegraph.throttling;

import java.util.Objects;

import javax.servlet.http.HttpServletRequest;

/**
 * Segmentation of requests by user-agent and IP address.
 */
public class UserAgentIpAddressBucketing implements Bucketing {

    /** {@inheritDoc} */
    @Override
    public Object bucket(HttpServletRequest request) {
        return new Bucket(request.getRemoteAddr(), request.getHeader("User-Agent"));
    }

    /**
     * A bucket based on user-agent and IP address.
     *
     * This is a simple class to wrap the user agent and IP address to avoid
     * doing string concatenation.
     */
    public static final class Bucket {
        /** IP address. */
        private final String remoteAddr;
        /** User-agent. */
        private final String userAgent;

        /**
         * Constructor.
         *
         * @param remoteAddr IP address
         * @param userAgent user-agent
         */
        private Bucket(String remoteAddr, String userAgent) {
            this.remoteAddr = remoteAddr;
            this.userAgent = userAgent;
        }

        /** {@inheritDoc} */
        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            Bucket bucket = (Bucket) o;

            return Objects.equals(remoteAddr, bucket.remoteAddr)
                    && Objects.equals(userAgent, bucket.userAgent);
        }

        /** {@inheritDoc} */
        @Override
        public int hashCode() {
            return Objects.hash(remoteAddr, userAgent);
        }
    }
}
