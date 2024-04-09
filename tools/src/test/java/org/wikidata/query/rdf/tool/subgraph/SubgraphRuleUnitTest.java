package org.wikidata.query.rdf.tool.subgraph;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.wikidata.query.rdf.tool.subgraph.SubgraphRule.TriplePattern.parseTriplePattern;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.openrdf.model.Resource;
import org.openrdf.model.URI;
import org.openrdf.model.ValueFactory;
import org.openrdf.model.impl.ValueFactoryImpl;

import com.google.common.collect.ImmutableMap;

@RunWith(Parameterized.class)
public class SubgraphRuleUnitTest {
    private static final ValueFactory VALUE_FACTORY = new ValueFactoryImpl();
    private final Map<String, String> prefixes = ImmutableMap.of(
            "wdt", "wikibase:direct#",
            "wd", "wikibase:entity#",
            "schema", "schema:");

    private final String expression;
    private final Object expected;

    public SubgraphRuleUnitTest(String expression, Object expected) {
        this.expression = expression;
        this.expected = expected;
    }

    @Parameterized.Parameters
    public static Collection<Object[]> testCases() {
        return Arrays.asList(
                new Object[]{"wd:Q42 wdt:P31 wd:Q5",
                        tp("wikibase:entity#Q42", "wikibase:direct#P31", "wikibase:entity#Q5")
                },
                new Object[]{"?entity wdt:P31 wd:Q5",
                        tp(VALUE_FACTORY.createBNode("entity"), "wikibase:direct#P31", "wikibase:entity#Q5")
                },
                new Object[]{"[] schema:about ?entity",
                        tp(VALUE_FACTORY.createBNode("wildcard"), "schema:about", VALUE_FACTORY.createBNode("entity"))
                },
                new Object[]{"[] missing:about ?entity", IllegalArgumentException.class},
                new Object[]{"[] wdt:P31 ?unknown", IllegalArgumentException.class},
                new Object[]{"garbage", IllegalArgumentException.class});
    }

    public static Resource resource(Object res) {
        if (res instanceof String) {
            res = VALUE_FACTORY.createURI((String) res);
        }
        return (Resource) res;
    }
    private static SubgraphRule.TriplePattern tp(Object subject, Object predicate, Object object) {
        return new SubgraphRule.TriplePattern(resource(subject), (URI) resource(predicate), resource(object));

    }

    @Test
    public void testParseTriplePatter() {
        if (expected instanceof Class) {
            assertThatThrownBy(() -> parseTriplePattern(expression, VALUE_FACTORY, prefixes)).isInstanceOf((Class<?>) expected);
        } else {
            assertThat(parseTriplePattern(expression, VALUE_FACTORY, prefixes)).isEqualTo(expected);
        }
    }
}
