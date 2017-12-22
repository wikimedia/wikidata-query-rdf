package org.wikidata.query.rdf.tool.wikibase;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING;
import static org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.INPUT_DATE_FORMAT;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class RecentChangeResponse extends WikibaseResponse {

    private final Continue aContinue;
    private final Query query;

    @JsonCreator
    public RecentChangeResponse(
            @JsonProperty("error") Object error,
            @JsonProperty("continue") Continue aContinue,
            @JsonProperty("query") Query query) {
        super(error);
        this.aContinue = aContinue;
        this.query = query;
    }

    public Continue getContinue() {
        return aContinue;
    }

    public Query getQuery() {
        return query;
    }

    public static class Query {
        private final List<RecentChange> recentChanges;

        @JsonCreator
        public Query(@JsonProperty("recentchanges") List<RecentChange> recentChanges) {
            this.recentChanges = recentChanges;
        }

        public List<RecentChange> getRecentChanges() {
            return recentChanges;
        }
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "EI_EXPOSE_REP2"}, justification = "We'll need to migrate to JSR310 at some point")
    public static class RecentChange {
        private final Long ns;
        private final String title;
        private final Date timestamp;
        private final Long revId;
        private final Long rcId;
        private final String type;

        @JsonCreator
        public RecentChange(
                @JsonProperty("ns") Long ns,
                @JsonProperty("title") String title,
                @JsonProperty("timestamp") @JsonFormat(shape = STRING, pattern = INPUT_DATE_FORMAT) Date timestamp,
                @JsonProperty("revid") Long revId,
                @JsonProperty("rcid") Long rcId,
                @JsonProperty("type") String type
        ) {
            this.ns = ns;
            this.title = title;
            this.timestamp = timestamp;
            this.revId = revId;
            this.rcId = rcId;
            this.type = type;
        }

        public Long getNs() {
            return ns;
        }

        public String getTitle() {
            return title;
        }

        public Date getTimestamp() {
            return timestamp;
        }

        public Long getRevId() {
            return revId;
        }

        public Long getRcId() {
            return rcId;
        }

        public String getType() {
            return type;
        }
    }
}
