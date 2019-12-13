package org.wikidata.query.rdf.tool.rdf.client;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.eclipse.jetty.client.api.ContentResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Attempts to log update response information but very likely only works
 * for Blazegraph.
 */
class UpdateCountResponseHandler implements ResponseHandler<Integer> {
    private static final Logger log = LoggerFactory.getLogger(UpdateCountResponseHandler.class);

    /**
     * The pattern for the response for an update.
     */
    private static final Pattern ELAPSED_LINE = Pattern.compile("><p>totalElapsed=[^ ]+ elapsed=([^<]+)</p");
    /**
     * The pattern for the response for an update, with extended times for clauses.
     */
    private static final Pattern ELAPSED_LINE_CLAUSES =
            Pattern.compile("><p>totalElapsed=([^ ]+) elapsed=([^ ]+) whereClause=([^ ]+) deleteClause=([^ ]+) insertClause=([^ <]+)</p");
    /**
     * The pattern for the response for an update, with extended times for clauses and flush.
     */
    private static final Pattern ELAPSED_LINE_FLUSH =
            Pattern.compile("><p>totalElapsed=([^ ]+) elapsed=([^ ]+) connFlush=([^ ]+) " +
                    "batchResolve=([^ ]+) whereClause=([^ ]+) deleteClause=([^ ]+) insertClause=([^ <]+)</p");
    /**
     * The pattern for the response for a commit.
     */
    private static final Pattern COMMIT_LINE = Pattern
            .compile("><hr><p>COMMIT: totalElapsed=([^ ]+) commitTime=[^ ]+ mutationCount=([^<]+)</p");
    /**
     * The pattern for the response from a bulk update.
     */
    private static final Pattern BULK_UPDATE_LINE = Pattern
            .compile("<\\?xml version=\"1.0\"\\?><data modified=\"(\\d+)\" milliseconds=\"(\\d+)\"/>");

    @Override
    public String acceptHeader() {
        return null;
    }

    @Override
    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "more readable with 2 calls")
    public Integer parse(ContentResponse entity) throws IOException {
        Integer mutationCount = null;
        String content = entity.getContentAsString();
        for (String line : content.split("\\r?\\n")) {
            Matcher m;
            m = ELAPSED_LINE_FLUSH.matcher(line);
            if (m.matches()) {
                log.debug("total = {} elapsed = {} flush = {} batch = {} where = {} delete = {} insert = {}",
                        m.group(1), m.group(2), m.group(3), m.group(4),
                        m.group(5), m.group(6), m.group(7));
                continue;
            }
            m = ELAPSED_LINE_CLAUSES.matcher(line);
            if (m.matches()) {
                log.debug("total = {} elapsed = {} where = {} delete = {} insert = {}",
                        m.group(1), m.group(2), m.group(3), m.group(4),
                        m.group(5));
                continue;
            }
            m = ELAPSED_LINE.matcher(line);
            if (m.matches()) {
                log.debug("elapsed = {}", m.group(1));
                continue;
            }
            m = COMMIT_LINE.matcher(line);
            if (m.matches()) {
                log.debug("total = {} mutation count = {} ", m.group(1),
                        m.group(2));
                mutationCount = Integer.valueOf(m.group(2));
                continue;
            }
            m = BULK_UPDATE_LINE.matcher(line);
            if (m.matches()) {
                log.debug("bulk updated {} items in {} millis", m.group(1),
                        m.group(2));
                mutationCount = Integer.valueOf(m.group(1));
                continue;
            }
        }
        if (mutationCount == null) {
            throw new IOException("Couldn't find the mutation count!");
        }
        return mutationCount;
    }
}
