package org.wikidata.query.rdf.blazegraph.label;

import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.wikidata.query.rdf.test.Matchers.assertResult;
import static org.wikidata.query.rdf.test.Matchers.binds;
import static org.wikidata.query.rdf.test.Matchers.notBinds;

import java.util.Locale;
import java.util.Set;

import org.hamcrest.Matcher;
import org.junit.Rule;
import org.junit.Test;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.BNodeImpl;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.query.BindingSet;
import org.openrdf.query.MalformedQueryException;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.openrdf.query.algebra.evaluation.QueryBindingSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.AbstractRandomizedBlazegraphTestBase;
import org.wikidata.query.rdf.blazegraph.geo.GeoUtils;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.RDFS;
import org.wikidata.query.rdf.common.uri.SKOS;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.test.Randomizer;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.Var;
import com.bigdata.rdf.sail.sparql.Bigdata2ASTSPARQLParser;
import com.bigdata.rdf.sparql.ast.ASTContainer;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.ASTEvalHelper;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.bigdata.rdf.store.BD;

public class LabelServiceUnitTest extends AbstractRandomizedBlazegraphTestBase {
    private static final Logger log = LoggerFactory.getLogger(LabelServiceUnitTest.class);

    @Rule
    public final Randomizer randomizer = new Randomizer();

    @Test
    public void labelOverConstant() throws QueryEvaluationException {
        simpleLabelLookupTestCase(null, "wd:Q123");
    }

    @Test
    public void labelOverVariable() throws QueryEvaluationException {
        add("ontology:dummy", "ontology:dummy", "wd:Q123");
        simpleLabelLookupTestCase("ontology:dummy ontology:dummy ?o.", "?o");
    }

    @Test
    public void chain() throws QueryEvaluationException {
        add("ontology:dummy", "ontology:dummy", "wd:Q1");
        add("wd:Q1", "ontology:dummy", "wd:Q2");
        add("wd:Q2", "ontology:dummy", "wd:Q3");
        add("wd:Q3", "ontology:dummy", "wd:Q4");
        add("wd:Q4", "ontology:dummy", "wd:Q123");
        simpleLabelLookupTestCase(
                "ontology:dummy ontology:dummy/ontology:dummy/ontology:dummy/ontology:dummy/ontology:dummy ?o.", "?o");
    }

    @Test
    public void many() throws QueryEvaluationException {
        for (int i = 1; i <= 10; i++) {
            addSimpleLabels("Q" + i);
            add("ontology:dummy", "ontology:dummy", "wd:Q" + i);
        }
        TupleQueryResult result = lookupLabel("ontology:dummy ontology:dummy ?o", "en", "?o", "rdfs:label");
        for (int i = 1; i <= 10; i++) {
            assertTrue(result.hasNext());
            assertThat(result.next(), binds("oLabel", new LiteralImpl("in en", "en")));
        }
        assertFalse(result.hasNext());
    }

    @Test
    public void labelOverUnboundSubject() throws QueryEvaluationException {
        TupleQueryResult result = lookupLabel(null, "en", "?s", "rdfs:label");
        assertThat(result.next(), notBinds("sLabel"));
        assertFalse(result.hasNext());
    }

    @Test
    public void noDotIsOkErrorMessage() {
        try {
            StringBuilder query = Ontology.prefix(new StringBuilder());
            query.append("SELECT *\n");
            query.append("WHERE {\n");
            query.append("  SERVICE ontology:label {}\n");
            query.append("}\n");
            query(query.toString());
            fail();
        } catch (IllegalArgumentException e) {
            assertThat(e.getMessage(), containsString("must provide the label service a list of languages"));
        }
    }

    @Test
    public void deeperServiceCall() {
        add("ontology:dummy", "ontology:dummy", "wd:Q1");
        add("wd:Q1", "ontology:dummy", "wd:Q123");
        addSimpleLabels("Q123");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?pLabel\n");
        query.append("WHERE {\n");
        query.append("  ontology:dummy ontology:dummy ?s .\n");
        query.append("  {\n");
        query.append("    ?s ontology:dummy ?p .\n");
        query.append("    SERVICE ontology:label { bd:serviceParam ontology:language \"en , de\" . }\n");
        query.append("  }\n");
        query.append("}\n");
        assertResult(query(query.toString()), binds("pLabel", "in en", "en"));
    }

    @Test
    public void deeperServiceCall_T153353_simple() {
        addSimpleLabels("Q153353");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p\n");
        query.append("WHERE {\n");
        query.append("    {\n");
        query.append("        SELECT ?p ?pLabel WHERE {\n");
        query.append("            BIND(wd:Q153353 AS ?p)\n");
        query.append("            SERVICE ontology:label { bd:serviceParam ontology:language \"en\" . }\n");
        query.append("        }\n");
        query.append("    }\n");
        query.append("    FILTER(\"in en\"@en = ?pLabel) .\n");
        query.append("}");
        assertResult(query(query.toString()), binds("p", URI.class));
    }

    // @Test
    // This test is disabled due to errors not directly related to Q153353,
    // on some occasions, results are duplicated.
    public void deeperServiceCall_T153353_nested() {
        addSimpleLabels("Q153353_nested");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabel\n");
        query.append("WHERE {\n");
        query.append("    {\n");
        query.append("        SELECT ?p ?pLabel WHERE {\n");
        query.append("            {{\n");
        query.append("            SERVICE ontology:label { bd:serviceParam ontology:language \"en\" . }\n");
        query.append("            }\n");
        query.append("            UNION");
        query.append("            {\n");
        query.append("            }}\n");
        query.append("        }\n");
        query.append("    }\n");
        query.append("    FILTER(\"in en\"@en = ?pLabel) .\n");
        query.append("}");
        query.append("VALUES (?p) {(wd:Q153353_nested)}\n");
        assertResult(query(query.toString()), binds("p", URI.class));
    }

    // Blazegraph throws RuntimeException wrapping various checked exceptions and it make it very useful
    // to print the query plan for debugging if any test fails
    @SuppressWarnings({"checkstyle:illegalcatch"})
    @SafeVarargs
    private final void assertLabelQueryResult(String query, final Matcher<BindingSet>... bindingMatchers) {
        ASTContainer astContainer = null;
        try {
            TupleQueryResult q;
            try {
                astContainer = new Bigdata2ASTSPARQLParser().parseQuery2(query, null);
                q = ASTEvalHelper.evaluateTupleQuery(store(), astContainer, new QueryBindingSet(), null);
            } catch (MalformedQueryException | QueryEvaluationException e) {
                throw new RuntimeException(e);
            }
            assertResult(q, bindingMatchers);
        } catch (AssertionError | RuntimeException e) {
            log.error("Error while checking results for {}", astContainer);
            throw e;
        }
    }

    @Test
    public void multipleServiceCall_T175840() {
        addSimpleLabels("Q175840sc");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabelEn ?pLabelDe\n");
        query.append("WHERE {\n");
        query.append("        SERVICE ontology:label { bd:serviceParam ontology:language \"en\" . \n");
        query.append("            ?p rdfs:label ?pLabelEn. }\n");
        query.append("        SERVICE ontology:label { bd:serviceParam ontology:language \"de\" . \n");
        query.append("            ?p rdfs:label ?pLabelDe. }\n");
        query.append("}");
        query.append("VALUES (?p) {(wd:Q175840sc)}\n");
        assertLabelQueryResult(query.toString(),
                both(binds("p", URI.class))
                .and(binds("pLabelEn", Literal.class))
                .and(binds("pLabelDe", Literal.class))
        );
    }

    @Test
    public void multipleServiceCall_T175840_with_optional() {
        addSimpleLabels("Q175840o");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabel \n");
        query.append("WHERE {\n");
        query.append("        SERVICE ontology:label { bd:serviceParam ontology:language \"en\" . }\n");
        query.append("        OPTIONAL { LET(?p:= wd:Q175840o) } \n");
        query.append("}");
        assertLabelQueryResult(query.toString(),
                both(binds("p", URI.class))
                .and(binds("pLabel", Literal.class))
        );
    }

     @Test
    public void multipleServiceCall_T175840_with_other_service() {
        addSimpleLabels("Q175840s3");
        add("wd:Q175840s1", "wdt:P625", GeoUtils.pointLiteral("Point(-180,-90)"));
        add("wd:Q175840s2", "wdt:P625", GeoUtils.pointLiteral("Point(180,90)"));
        add("wd:Q175840s3", "wdt:P625", GeoUtils.pointLiteral("Point(0,0)"));
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?item ?name ?coord \n");
        query.append("WHERE {\n");
        query.append("        wd:Q175840s1 wdt:P625 ?Firstloc .");
        query.append("        wd:Q175840s2 wdt:P625 ?Secondloc .");
        query.append("        SERVICE ontology:box { ?item wdt:P625 ?coord .\n");
        query.append("            bd:serviceParam ontology:cornerSouthWest ?Firstloc .\n");
        query.append("            bd:serviceParam ontology:cornerNorthEast ?Secondloc .\n");
        query.append("         } \n");
        query.append("        SERVICE ontology:label { \n");
        query.append("            bd:serviceParam ontology:language \"en\" . \n");
        query.append("            ?item rdfs:label ?name \n");
        query.append("        } \n");
        query.append("    FILTER(\"in en\"@en = ?name) .\n");
        query.append("} ORDER BY ASC(?name)");
        assertLabelQueryResult(query.toString(),
                 both(binds("item", URI.class))
                 .and(binds("name", Literal.class))
        );
    }

     @Test
    public void multipleServiceCall_T175840_with_filters() {
        addSimpleLabels("PROP");
        addSimpleLabels("PS");
        addSimpleLabels("Q175840fv");
        add("wd:Q175840f", "wdt:PROP", "wdt:Q175840fv");
        add("wd:PS", "ontology:directClaim", "wdt:PROP");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?pLabel ?prop ?val ?valLabel WHERE { \n");
        query.append(" wd:Q175840f ?prop ?val .");
        query.append("        ?ps ontology:directClaim ?prop .");
        query.append("        ?ps rdfs:label ?pLabel .");
        query.append("        FILTER (?prop != wdt:P18) .\n");
        query.append("         SERVICE ontology:label { \n");
        query.append("            bd:serviceParam ontology:language \"en\" . \n");
        // The testcase fails if variables are not explicitely defined in the service call
        // due to FILTER clause could not see bound variable while ASTBottomUpOptimizer is running,
        // so no proper joins are generated, but on the other hand, if projection vars
        // are automatically added for service call before ASTBottomUpOptimizer,
        // many other usecases fail due these vars expected to be projected,
        // but there are no method to assign inbound vars for the service call except
        // its statement patterns.
        query.append("            ?p rdfs:label ?pLabel . \n");
        query.append("            ?val rdfs:label ?valLabel . \n");
        query.append("        } \n");
        query.append("    FILTER(LANG(?valLabel) != 'fr') .\n");
        query.append("}");
        assertLabelQueryResult(query.toString(),
                 both(binds("prop", URI.class))
                    .and(binds("pLabel", Literal.class))
                    .and(binds("val", URI.class))
                    .and(binds("valLabel", Literal.class)),
                 both(binds("prop", URI.class))
                    .and(binds("pLabel", Literal.class))
                    .and(binds("val", URI.class))
                    .and(binds("valLabel", Literal.class)),
                 both(binds("prop", URI.class))
                    .and(binds("pLabel", Literal.class))
                    .and(binds("val", URI.class))
                    .and(binds("valLabel", Literal.class))

         );
    }

    @Test
    public void multipleServiceCall_T175840_subquery() {
        addSimpleLabels("Q175840SUB");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabel WHERE { \n" +
                "  { \n" +
                "    SELECT ?p WHERE {\n" +
                "      BIND(wd:Q175840SUB as ?p) \n" +
                "    } \n" +
                "  } \n" +
                "  SERVICE ontology:label { bd:serviceParam ontology:language \"en\" } \n" +
                "}");
        assertLabelQueryResult(query.toString(),
            both(binds("p", URI.class))
            .and(binds("pLabel", Literal.class))
        );
    }

    @Test
    public void multipleServiceCall_T175840_named_subquery() {
        addSimpleLabels("Q175840SUB");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabel WITH \n" +
                "  { \n" +
                "    SELECT ?p WHERE {\n" +
                "      BIND(wd:Q175840SUB as ?p) \n" +
                "    } \n" +
                "  } as %result \n" +
                "   WHERE { INCLUDE %result \n" +
                "  SERVICE ontology:label { bd:serviceParam ontology:language \"en\" } \n" +
                "}");
        assertLabelQueryResult(query.toString(),
            both(binds("p", URI.class))
            .and(binds("pLabel", Literal.class))
        );
    }

    @Test
    public void multipleServiceCall_T175840_propertyPath() {
        addSimpleLabels("Q175840PP1");
        add("wd:Q175840PP2", "wdt:P31", "wd:Q175840PP1");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?p ?pLabel WHERE { \n" +
                "  wd:Q175840PP2 wdt:P31+ ?p . \n" +
                "  SERVICE ontology:label { bd:serviceParam ontology:language \"en\" } \n" +
                "}");
        assertLabelQueryResult(query.toString(),
            both(binds("p", URI.class))
            .and(binds("pLabel", Literal.class))
        );
    }

    @Test
    public void unionWithServiceCall_T159723_binds_union() {
        // NotMaterializedException when one branch of UNION binds ?variable and other branch binds ?variableLabel and label service is used
        // https://phabricator.wikimedia.org/T159723
        addSimpleLabels("Q159723U");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?x ?xLabel WHERE {\n" +
                "        { BIND(wd:Q159723U AS ?x). } UNION\n" +
                "        { BIND(\"none\"@en AS ?xLabel). }\n" +
                "        SERVICE ontology:label { bd:serviceParam ontology:language \"en\". }\n" +
                "      }");
        assertLabelQueryResult(query.toString(),
            both(binds("xLabel", Literal.class))
            .and(binds("x", URI.class)), // First solution contains both x bound by BIND and xLabel provided by the Service
            both(binds("xLabel", Literal.class))
            .and(notBinds("x")) // Second solution binds only xLabel bound by BIND and does not bind x
        );
    }

    @Test
    public void unionWithServiceCall_T159723_binds() {
        // NotMaterializedException when one branch of UNION binds ?variable and other branch binds ?variableLabel and label service is used
        // https://phabricator.wikimedia.org/T159723
        addSimpleLabels("Q159723B");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?x ?xLabel WHERE {\n" +
                "        BIND(wd:Q159723B AS ?x).\n" +
                "        BIND(\"Douglas Adams\"@en AS ?xLabel).\n" +
                "        SERVICE ontology:label { bd:serviceParam ontology:language \"en\". }\n" +
                "      }");
        assertLabelQueryResult(query.toString(),
            both(binds("xLabel", Literal.class))
            .and(binds("x", URI.class))
        );
    }

      @Test
      public void unionWithServiceCall_T159723_values() {
          // NotMaterializedException when one branch of UNION binds ?variable and other branch binds ?variableLabel and label service is used
          // https://phabricator.wikimedia.org/T159723
          addSimpleLabels("Q159723V");
          StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
          query.append("SELECT ?x ?xLabel WHERE {\n" +
                  "        VALUES (?x ?xLabel) {\n" +
                  "          (wd:Q159723V \"Douglas Adams\"@en)\n" +
                  "        }\n" +
                  "        SERVICE ontology:label { bd:serviceParam ontology:language \"en\". }\n" +
                  "      }");
          assertLabelQueryResult(query.toString(),
                  both(binds("xLabel", Literal.class))
                  .and(binds("x", URI.class))
          );
      }

    @Test
    public void sameVarAssignment_T170704() {
        // NME when using label service and rdfs:label predicate with the same variable
        // https://phabricator.wikimedia.org/T170704
        addSimpleLabels("Q170704");
        addSimpleLabels("Q170704P31");
        add("wd:Q170704", "wdt:P31", "wd:Q170704P31");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?item ?itemLabel ?instance_of ?instance_ofLabel WHERE {\n" +
                "            ?item wdt:P31 wd:Q170704P31.\n" +
                "            SERVICE ontology:label { bd:serviceParam ontology:language \"en\". }\n" +
                "            ?item wdt:P31 ?instance_of.\n" +
                "            ?instance_of rdfs:label ?instance_ofLabel.\n" +
                "            FILTER((LANG(?instance_ofLabel)) = \"en\")\n" +
                "          }");
        assertLabelQueryResult(query.toString(),
            both(binds("item", URI.class))
            .and(binds("itemLabel", Literal.class))
            .and(binds("instance_of", URI.class))
            .and(binds("instance_ofLabel", Literal.class))
        );
    }

    private void simpleLabelLookupTestCase(String extraQuery, String subjectInQuery) throws QueryEvaluationException {
        addSimpleLabels("Q123");
        slltcp(extraQuery, subjectInQuery, "en", "in en", "en", "alt label in en, alt label in en2", "en");
        slltcp(extraQuery, subjectInQuery, "ru", "in ru", "ru", null, null);
        slltcp(extraQuery, subjectInQuery, "dummy", "Q123", null, null, null);
        slltcp(extraQuery, subjectInQuery, "dummy.en", "in en", "en", "alt label in en, alt label in en2", "en");
        slltcp(extraQuery, subjectInQuery, "en.ru", "in en", "en", "alt label in en, alt label in en2", "en");
        slltcp(extraQuery, subjectInQuery, "ru.de", "in ru", "ru", "alt label in de", "de");
        slltcp(extraQuery, subjectInQuery, "en.ru.en", "in en", "en", "alt label in en, alt label in en2", "en");
    }

    private void slltcp(String extraQuery, String subjectInQuery, String language, String labelText,
            String labelLanguage, String altLabelText, String altLabelLanguage) throws QueryEvaluationException {
        assertResult(
                lookupLabel(extraQuery, language, subjectInQuery, "rdfs:label", "skos:altLabel"),
                both(
                        binds(labelName(subjectInQuery, "rdfs:label"), labelText, labelLanguage)
                    ).and(
                        binds(labelName(subjectInQuery, "skos:altLabel"), altLabelText, altLabelLanguage)
                    )
        );

    }

    private String languageParams(String inLanguages) {
        String[] langs;
        StringBuilder params = new StringBuilder();
        if (inLanguages.contains(".")) {
            langs = inLanguages.split("\\.");
        } else {
            langs = new String[] {inLanguages};
        }
        for (String lang: langs) {
            params.append("bd:serviceParam ontology:language \"" + lang + "\".\n");
        }
        return params.toString();
    }

    private TupleQueryResult lookupLabel(String otherQuery, String inLanguages, String subject, String... labelTypes)
            throws QueryEvaluationException {
        if (inLanguages.indexOf(' ') >= 0) {
            throw new IllegalArgumentException("Languages cannot contain a space or that'd make an invalid query.");
        }
        StringBuilder query = uris().prefixes(
                SchemaDotOrg.prefix(SKOS.prefix(RDFS.prefix(Ontology.prefix(new StringBuilder())))));
        query.append("SELECT");
        for (String labelType : labelTypes) {
            query.append(" ?").append(labelName(subject, labelType));
        }
        query.append('\n');
        query.append("WHERE {\n");
        if (otherQuery != null) {
            query.append(otherQuery).append("\n");
        }
        query.append("  SERVICE ontology:label {\n").append(languageParams(inLanguages));
        if (subject.contains(":") || randomizer.rarely()) {
            // We rarely explicitly specify the labels to load
            for (String labelType : labelTypes) {
                query.append("    ").append(subject).append(" ").append(labelType).append(" ?")
                        .append(labelName(subject, labelType)).append(" .\n");
            }
        }
        query.append("  }\n");
        query.append("}\n");
        log.debug("Query: {}", query);
        return query(query.toString());
    }

    private void addSimpleLabels(String entity) {
        for (String language : new String[] {"en", "de", "ru"}) {
            add("wd:" + entity, RDFS.LABEL, new LiteralImpl("in " + language, language));
        }
        add("wd:" + entity, SKOS.ALT_LABEL, new LiteralImpl("alt label in en", "en"));
        add("wd:" + entity, SKOS.ALT_LABEL, new LiteralImpl("alt label in en2", "en"));
        add("wd:" + entity, SKOS.ALT_LABEL, new LiteralImpl("alt label in de", "de"));
        for (String language : new String[] {"en", "de", "ru"}) {
            add("wd:" + entity, SchemaDotOrg.DESCRIPTION, new LiteralImpl("description in " + language, language));
        }
    }

    private String labelName(String subjectName, String labelType) {
        int start = labelType.indexOf(':') + 1;
        if (subjectName.contains(":")) {
            return labelType.substring(start);
        }
        return subjectName.substring(1) + labelType.substring(start, start + 1).toUpperCase(Locale.ROOT)
                + labelType.substring(start + 1);
    }

    @Test
    public void labelOnAsk() {
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("ASK {\n");
        query.append("  ontology:dummy ontology:dummy ?s .\n");
        query.append("  SERVICE ontology:label { bd:serviceParam ontology:language \"en,de\" . }\n");
        query.append("}\n");
        assertFalse(ask(query.toString()));
    }

    @SuppressWarnings("unchecked")
    @Test
    public void desiredVars() {
        JoinGroupNode patterns = new JoinGroupNode();
        // Label
        patterns.addArg(new StatementPatternNode(
                new VarNode("item"),
                createURI(RDFS.LABEL),
                new VarNode("itemLabel")
        ));
        // Description
        patterns.addArg(new StatementPatternNode(
                new VarNode("item2"),
                createURI(SchemaDotOrg.DESCRIPTION),
                new VarNode("itemDesc")
        ));
        // Fixed name
        patterns.addArg(new StatementPatternNode(
                createURI(uris().entityIdToURI("Q123")),
                createURI(RDFS.LABEL),
                new VarNode("qLabel")
        ));
        // Parameters
        patterns.addArg(new StatementPatternNode(
                createURI(BD.SERVICE_PARAM),
                createURI(LabelService.LANGUAGE_PARAM),
                createConstant("en,fr")
        ));
        ServiceNode serviceNode = new ServiceNode(createURI(LabelService.SERVICE_KEY), patterns);

        final LabelService service = new LabelService();
        Set<IVariable<?>> vars = service.getDesiredBound(serviceNode);
        assertThat(vars, hasSize(2));

        assertThat(vars, hasItems(
                            equalTo(Var.var("item")),
                            equalTo(Var.var("item2"))));
    }

    @Test
    public void labelWildcardAndBind() throws QueryEvaluationException {
        addSimpleLabels("Q123");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT * WHERE {\n" +
                "  BIND(wd:Q123 AS ?item)\n" +
                "  SERVICE ontology:label {\n" +
                "    bd:serviceParam ontology:language \"en\".\n" +
                "    ?item rdfs:label ?itemLabel.\n" +
                "  }\n" +
                "  hint:Prior hint:runLast false .\n" +
                "  BIND(?itemLabel as ?anotherLabel)\n" +
                "}");
        QueryResult result = queryWithAST(query.toString());
        try {
            assertTrue(result.hasNext());
            BindingSet resultSet = result.next();
            assertThat(resultSet, both(
                    binds("itemLabel", "in en", "en")
                    ).and(
                            binds("anotherLabel", "in en", "en")
                    )
            );
        } catch (AssertionError e) {
            log.error("Error while checking results for " + result.ast());
            throw e;
        }
    }

    private void checkSpecialLabel(String binding, String resultLabel) throws QueryEvaluationException {
        addSimpleLabels("Q123");
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?item ?itemLabel WHERE {\n" +
                "  BIND(" + binding + " AS ?item)\n" +
                "  SERVICE ontology:label {\n" +
                "    bd:serviceParam ontology:language \"en\".\n" +
                "  }\n" +
                "}");
        QueryResult result = queryWithAST(query.toString());
        try {
            assertTrue(result.hasNext());
            BindingSet resultSet = result.next();
            assertThat(resultSet, binds("itemLabel", resultLabel, null));
        } catch (AssertionError e) {
            log.error("Error while checking results for " + result.ast());
            throw e;
        }
    }

    @Test
    public void labelWhenMissing() throws QueryEvaluationException {
        checkSpecialLabel("wd:Q3456", "Q3456");
    }

    @Test
    public void labelFromOtherTypes() throws QueryEvaluationException {
        checkSpecialLabel("STR(wd:Q3456)", "Q3456");
        // TODO: We may want to verify that's what we want
        checkSpecialLabel("STR(wd:Q123)", "Q123");
        checkSpecialLabel("\"some string\"", "some string");
        // TODO: do we want to strip language?
        checkSpecialLabel("\"some string\"@ru", "some string");
        checkSpecialLabel("123", "123");
        checkSpecialLabel("IRI(\"http://www.wikidata.org/\")", "http://www.wikidata.org/");
        checkSpecialLabel("\"2018-11-08T23:29:06Z\"^^xsd:dateTime", "2018-11-08T23:29:06Z");
        checkSpecialLabel("bnode(\"test\")", "-bnode-func-test");
    }

    private void checkOtherType(Object o, String expectedResult) throws QueryEvaluationException {
        add("wd:Q888", "p:P999", o);
        StringBuilder query = uris().prefixes(Ontology.prefix(new StringBuilder()));
        query.append("SELECT ?item ?itemLabel WHERE {\n" +
                " wd:Q888 p:P999 ?item .\n" +
                "  SERVICE ontology:label {\n" +
                "    bd:serviceParam ontology:language \"en\".\n" +
                "  }\n" +
                "}");
        TupleQueryResult result = query(query.toString());
        assertTrue(result.hasNext());
        BindingSet resultSet = result.next();
        assertThat(resultSet, binds("itemLabel", expectedResult, null));
        closeStore();
    }

    @Test
    public void labelFromOtherTypesQuery() throws QueryEvaluationException {
        checkOtherType(new LiteralImpl(uris().entityIdToURI("Q123")), "Q123");
        checkOtherType(new LiteralImpl("Q123"), "Q123");
        checkOtherType(new LiteralImpl("just testing", "ru"), "just testing");
        checkOtherType(new LiteralImpl("http://www.wikidata.org/"), "http://www.wikidata.org/");
        checkOtherType(new URIImpl(uris().entityIdToURI("Q234")), "Q234");
        checkOtherType(new URIImpl("http://www.wikidata.org/"), "http://www.wikidata.org/");
        checkOtherType(new BNodeImpl("Q234"), "t1");
    }
}
