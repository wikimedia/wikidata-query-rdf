package org.wikidata.query.rdf.tool.rdf;

import static com.google.common.collect.Lists.newArrayList;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.TreeSet;

import org.apache.commons.io.IOUtils;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.PropertyType;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

import com.google.common.collect.Sets;

/**
 * This is basically a smoke test that ensures that no changes to the multisync query were made unintentionally.
 */
@RunWith(Parameterized.class)
public class MultiSyncUpdateQueryFactoryUnitTest {

    private static final Instant TIMESTAMP_PARAM = Instant.ofEpochMilli(1578391403141L);
    private final Set<String> values;
    private final Set<String> refs;

    private UrisScheme urisScheme;
    private String expectedQuery;

    @Parameterized.Parameters(name = "expected query file - {2}")
    public static Collection<Object[]> params() {
        return newArrayList(
                new Object[] {sortedSet("12", "13"), sortedSet("ref1", "ref2"), "query_refs_vals.txt"},
                new Object[] {sortedSet(), sortedSet("ref1", "ref2"), "query_refs_only.txt"},
                new Object[] {sortedSet("12", "13"), sortedSet(), "query_vals_only.txt"},
                new Object[] {sortedSet(), sortedSet(), "query_no_refs_vals.txt"}
        );
    }

    public MultiSyncUpdateQueryFactoryUnitTest(Set<String> values, Set<String> refs, String queryFile) throws IOException {
        this.values = values;
        this.refs = refs;
        urisScheme = UrisSchemeFactory.forHost("acme.test");
        expectedQuery = IOUtils.toString(getClass().getResourceAsStream(queryFile), UTF_8);
    }

    @Test
    public void shouldConstructCorrectQuery() {
        MultiSyncUpdateQueryFactory multiSyncUpdateQueryFactory = new MultiSyncUpdateQueryFactory(urisScheme);


        ClassifiedStatements classifiedStatements = new ClassifiedStatements(urisScheme);
        classifiedStatements.aboutStatements.add(createStatement("Q1", PropertyType.STATEMENT_VALUE, "Some value"));
        classifiedStatements.entityStatements.add(createStatement("Q4", PropertyType.DIRECT, "entity"));
        classifiedStatements.statementStatements.add(createStatement("Q5", PropertyType.STATEMENT, "Statement"));
        TreeSet<String> entityIds = sortedSet("Q1", "Q2", "Q3");

        ArrayList<Statement> insertStatements = newArrayList(
                createStatement("Q1", PropertyType.DIRECT, "Q2"),
                createStatement("Q2", PropertyType.CLAIM, "Q3")
        );
        String query = multiSyncUpdateQueryFactory.buildQuery(entityIds,
                insertStatements,
                classifiedStatements,
                values,
                refs,
                newArrayList("lex1", "lex2"),
                TIMESTAMP_PARAM
        );

        assertThat(query).isEqualTo(expectedQuery);
    }

    private StatementImpl createStatement(String resourceId, PropertyType propertyType, String valueId) {
        return new StatementImpl(uriFromResourceId(resourceId), new URIImpl(urisScheme.property(propertyType)), uriFromResourceId(valueId));
    }

    private URI uriFromResourceId(String resourceId) {
        return new URIImpl(urisScheme.entityIdToURI(resourceId));
    }

    private static TreeSet<String> sortedSet(String... strings) {
        return Sets.newTreeSet(Sets.newHashSet(strings));
    }
}
