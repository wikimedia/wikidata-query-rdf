package org.wikidata.query.rdf.tool.rdf.client;


import lombok.Builder;
import lombok.Data;

@Data
@Builder
public class UpdateMetrics {

    public static final UpdateMetrics ZERO_METRICS = UpdateMetrics.builder().build();

    private final Integer totalElapsed;
    private final Integer elapsed;
    private final Integer connFlush;
    private final Integer batchResolve;
    private final Integer whereClause;
    private final Integer deleteClause;
    private final Integer insertClause;

    public UpdateMetrics add(UpdateMetrics other) {
        return UpdateMetrics.builder()
               .totalElapsed(sumNullable(other.totalElapsed, this.totalElapsed))
               .elapsed(sumNullable(this.elapsed, other.elapsed))
               .connFlush(sumNullable(this.connFlush, other.connFlush))
               .batchResolve(sumNullable(this.batchResolve, other.batchResolve))
               .whereClause(sumNullable(this.whereClause, other.whereClause))
               .deleteClause(sumNullable(this.deleteClause, other.deleteClause))
               .insertClause(sumNullable(this.insertClause, other.insertClause))
               .build();
    }

    private Integer sumNullable(Integer otherValue, Integer thisValue) {
        if (otherValue == null && thisValue == null) return null;
        return otherValue == null ? thisValue : onNull0(thisValue) + otherValue;
    }

    private Integer onNull0(Integer value) {
        return value != null ? value : 0;
    }
}
