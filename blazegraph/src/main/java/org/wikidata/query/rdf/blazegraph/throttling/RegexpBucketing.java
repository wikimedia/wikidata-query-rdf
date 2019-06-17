package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.collect.ImmutableList.copyOf;

import java.util.Collection;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.annotation.concurrent.Immutable;
import javax.annotation.concurrent.ThreadSafe;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket queries by regexp.
 */
@Immutable @ThreadSafe
public class RegexpBucketing implements Bucketing {
    private static final Logger log = LoggerFactory.getLogger(RegexpBucketing.class);

    /**
     * List of known patterns.
     */
    private final Collection<Pattern> patterns;
    /**
     * Function to extract necessary data from request.
     */
    private final Function<HttpServletRequest, String> requestExtract;

    public RegexpBucketing(Collection<Pattern> patterns, Function<HttpServletRequest, String> requestParameter) {
        this.patterns = copyOf(patterns);
        this.requestExtract = requestParameter;
    }

    @Override
    public Object bucket(HttpServletRequest request) {
        String requestData = requestExtract.apply(request);
        if (requestData == null) {
            return null;
        }
        for (Pattern p : patterns) {
            Matcher m = p.matcher(requestData);
            if (m.find()) {
                log.debug("Matched pattern {} on {}", p, requestData);
                return p;
            }
        }
        return null;
    }
}
