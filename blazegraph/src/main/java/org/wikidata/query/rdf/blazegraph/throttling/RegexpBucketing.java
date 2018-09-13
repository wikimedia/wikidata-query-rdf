package org.wikidata.query.rdf.blazegraph.throttling;

import static com.google.common.collect.ImmutableList.copyOf;

import java.util.Collection;
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

    public RegexpBucketing(Collection<Pattern> patterns) {
        this.patterns = copyOf(patterns);
    }

    @Override
    public Object bucket(HttpServletRequest request) {
        String query = request.getParameter("query");
        if (query == null) {
            return null;
        }
        for (Pattern p : patterns) {
            Matcher m = p.matcher(query);
            if (m.find()) {
                log.debug("Matched pattern {} on {}", p, query);
                return p;
            }
        }
        return null;
    }
}
