package org.wikidata.query.rdf.blazegraph.throttling;

import java.util.Collection;
import java.util.LinkedList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Bucket queries by regexp.
 */
public class RegexpBucketing implements Bucketing {
    private static final Logger log = LoggerFactory.getLogger(RegexpBucketing.class);

    /**
     * List of known patterns.
     */
    private Collection<Pattern> patterns = new LinkedList<>();

    public void setPatterns(Collection<Pattern> patterns) {
        this.patterns = patterns;
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
