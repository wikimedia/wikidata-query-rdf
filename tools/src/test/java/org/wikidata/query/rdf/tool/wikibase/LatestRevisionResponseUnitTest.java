package org.wikidata.query.rdf.tool.wikibase;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.util.Optional;

import org.junit.Test;
import org.wikidata.query.rdf.tool.MapperUtils;

public class LatestRevisionResponseUnitTest {
    private static final LatestRevisionResponse RESPONSE_WITH_PAGEIDS = new LatestRevisionResponse(
            new LatestRevisionResponse.Query(asList(
                    new LatestRevisionResponse.Page(123L, null, singletonList(new LatestRevisionResponse.Revision(321)), false),
                    new LatestRevisionResponse.Page(124L, null, null, true)
            )), null);
    private static final LatestRevisionResponse RESPONSE_WITH_TITLES = new LatestRevisionResponse(
                new LatestRevisionResponse.Query(asList(
            new LatestRevisionResponse.Page(null, "Q123", singletonList(new LatestRevisionResponse.Revision(321)), false),
            new LatestRevisionResponse.Page(null, "Q124", null, true)
            )), null);

    private static final LatestRevisionResponse RESPONSE_WITH_ERROR = new LatestRevisionResponse(null, new WikibaseApiError("oops", "oops"));
    @Test
    public void test_deserialize_json() throws IOException {
        LatestRevisionResponse response = MapperUtils.getObjectMapper().readValue(this.getClass().getResourceAsStream("latest_revision_response.json"),
                LatestRevisionResponse.class);
        assertThat(response.getQuery().getPages()).hasSize(2);
        assertThat(response.getQuery().getPages().get(0).isMissing()).isTrue();
        assertThat(response.getQuery().getPages().get(0).getTitle()).isEqualTo("Q9283378273782");
        assertThat(response.getQuery().getPages().get(0).getRevisions()).isNull();
        assertThat(response.getQuery().getPages().get(1).isMissing()).isFalse();
        assertThat(response.getQuery().getPages().get(1).getTitle()).isEqualTo("Q3");
        assertThat(response.getQuery().getPages().get(1).getPageid()).isEqualTo(131);
        assertThat(response.getQuery().getPages().get(1).getRevisions()).hasSize(1);
        assertThat(response.getQuery().getPages().get(1).getRevisions().get(0).getRevid()).isEqualTo(1539723790);
    }

    @Test
    public void test_deserialize_json_with_error() throws IOException {
        LatestRevisionResponse response = MapperUtils.getObjectMapper().readValue(this.getClass().getResourceAsStream("latest_revision_response_error.json"),
                LatestRevisionResponse.class);
        assertThat(response.getError()).isNotNull();
        assertThat(response.getError().getCode()).isEqualTo("multisource");
        assertThat(response.getError().toString()).contains("parameter cannot be used with");
        assertThat(response.getQuery()).isNull();
    }

    @Test
    public void test_find_by_pageid() {
        assertThat(RESPONSE_WITH_PAGEIDS.latestRevisionForPageid(123L)).isEqualTo(Optional.of(321L));
        assertThat(RESPONSE_WITH_PAGEIDS.latestRevisionForPageid(124L)).isEmpty();
        assertThatThrownBy(() -> RESPONSE_WITH_PAGEIDS.latestRevisionForPageid(125L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RESPONSE_WITH_TITLES.latestRevisionForPageid(123L)).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RESPONSE_WITH_ERROR.latestRevisionForPageid(125L)).isInstanceOf(IllegalStateException.class);
    }

    @Test
    public void test_find_by_title() {
        assertThat(RESPONSE_WITH_TITLES.latestRevisionForTitle("Q123")).isEqualTo(Optional.of(321L));
        assertThat(RESPONSE_WITH_TITLES.latestRevisionForTitle("Q124")).isEmpty();
        assertThatThrownBy(() -> RESPONSE_WITH_TITLES.latestRevisionForTitle("Q125")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RESPONSE_WITH_PAGEIDS.latestRevisionForTitle("Q123")).isInstanceOf(IllegalStateException.class);
        assertThatThrownBy(() -> RESPONSE_WITH_ERROR.latestRevisionForTitle("Q123")).isInstanceOf(IllegalStateException.class);
    }
}
