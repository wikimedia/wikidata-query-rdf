package org.wikidata.query.rdf.tool.wikibase;

import static com.fasterxml.jackson.annotation.JsonFormat.Shape.STRING;

import java.time.Instant;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

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

    public static class RecentChange {
        private final Long ns;
        private final String title;
        private final Instant timestamp;
        private final Long revId;
        private final Long rcId;
        private final String type;

        @JsonCreator
        public RecentChange(
                @JsonProperty("ns") Long ns,
                @JsonProperty("title") String title,
                @JsonProperty("timestamp") @JsonFormat(shape = STRING) Instant timestamp,
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

        public Instant getTimestamp() {
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
