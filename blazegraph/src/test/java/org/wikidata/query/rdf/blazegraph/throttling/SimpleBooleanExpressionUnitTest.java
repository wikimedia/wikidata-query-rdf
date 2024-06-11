package org.wikidata.query.rdf.blazegraph.throttling;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.google.common.collect.Sets;

import lombok.AllArgsConstructor;

@AllArgsConstructor
@RunWith(Parameterized.class)
public class SimpleBooleanExpressionUnitTest {
    private final Boolean expect;
    private final String expression;
    private final Set<String> context;

    @Parameterized.Parameters()
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[][] {
            // variable key length is accepted
            {true, "x-a", Collections.singleton("x-a")},
            // negation on single element
            {true, "a", Collections.singleton("a")},
            {false, "!a", Collections.singleton("a")},
            {false, "b", Collections.singleton("a")},
            {true, "!b", Collections.singleton("a")},
            // multiple elements
            {true, "a && b", Sets.newHashSet("a", "b")},
            {false, "a && !b", Sets.newHashSet("a", "b")},
            {false, "a && z", Sets.newHashSet("a", "b")},
            {true, "a && !z", Sets.newHashSet("a", "b")},
            // multiple elements with one missing
            {false, "z && a", Collections.singleton("a")},
            {true, "!z && a", Collections.singleton("a")},
            // flexible with whitespace
            {true, "a&&a", Collections.singleton("a")},
            {true, "a &&a", Collections.singleton("a")},
            {true, "a&& a", Collections.singleton("a")},
            {true, "a  && a", Collections.singleton("a")},
            // invalid expressions
            {null, "a || b", Collections.singleton("a")},
            {null, "invalid!", Collections.singleton("a")},
            {null, "!!invalid", Collections.singleton("a")}
        });
    }

    @Test
    public void test() {
        if (expect == null) {
            assertThatThrownBy(() -> SimpleBooleanExpression.create(expression));
        } else {
            SimpleBooleanExpression expr = SimpleBooleanExpression.create(expression);
            assertThat(expr.evaluate(context::contains)).isEqualTo(expect);
        }
    }
}
