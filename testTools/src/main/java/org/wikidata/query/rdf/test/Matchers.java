package org.wikidata.query.rdf.test;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.test.StatementHelper.uri;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.openrdf.model.Literal;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
import org.openrdf.query.QueryEvaluationException;
import org.openrdf.query.TupleQueryResult;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

/**
 * Useful matchers for RDF.
 */
public final class Matchers {
    /**
     * Check a binding to a uri.
     */
    public static Matcher<BindingSet> binds(String name, String value) {
        if (value.startsWith("P")) {
            value = WikibaseUris.getURISystem().property(PropertyType.CLAIM) + value;
        }
        return new BindsMatcher<URI>(name, equalTo(uri(value)));
    }

    /**
     * Check binding to specific class.
     */
    public static Matcher<BindingSet> binds(String name, Class<?> value) {
        return new BindsMatcher<Object>(name, instanceOf(value));
    }

    /**
     * Check a binding to a value.
     */
    public static <V> Matcher<BindingSet> binds(String name, V value) {
        return new BindsMatcher<V>(name, equalTo(value));
    }

    /**
     * Check a binding to a language string.
     */
    public static Matcher<BindingSet> binds(String name, String str, String language) {
        if (str == null && language == null) {
            return notBinds(name);
        }
        return new BindsMatcher<Literal>(name, equalTo((Literal) new LiteralImpl(str, language)));
    }

    /**
     * Check that a binding isn't bound.
     */
    public static Matcher<BindingSet> notBinds(String name) {
        return new NotBindsMatcher(name);
    }

    /**
     * Construct subject, predicate, and object matchers for a bunch of
     * statements.
     */
    @SuppressWarnings("unchecked")
    public static Matcher<BindingSet>[] subjectPredicateObjectMatchers(Iterable<Statement> statements) {
        List<Matcher<? super BindingSet>> matchers = new ArrayList<>();
        for (Statement statement : statements) {
            matchers.add(allOf(//
                    binds("s", statement.getSubject()), //
                    binds("p", statement.getPredicate()), //
                    binds("o", statement.getObject()) //
            ));
        }
        return (Matcher<BindingSet>[]) matchers.toArray(new Matcher<?>[matchers.size()]);
    }

    /**
     * Assert that a result set contains some result bindings. Not a matcher
     * because it modifies the result by iterating it.
     */
    @SafeVarargs
    public static void assertResult(TupleQueryResult result, Matcher<BindingSet>... bindingMatchers) {
        try {
            int position = 0;
            for (Matcher<BindingSet> bindingMatcher : bindingMatchers) {
                assertTrue("There should be at least " + position + " results", result.hasNext());
                assertThat(result.next(), bindingMatcher);
                position++;
            }
            assertFalse("There should be no more than " + position + " result", result.hasNext());
            result.close();
        } catch (QueryEvaluationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks bindings.
     *
     * @param <V> type of the binding result
     */
    private static class BindsMatcher<V> extends TypeSafeMatcher<BindingSet> {
        /**
         * Name of the binding to check.
         */
        private final String name;
        /**
         * Delegate matcher for the bound value.
         */
        private final Matcher<V> valueMatcher;

        public BindsMatcher(String name, Matcher<V> valueMatcher) {
            this.name = name;
            this.valueMatcher = valueMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("contains a binding from").appendValue(name).appendText("to")
                    .appendDescriptionOf(valueMatcher);
        }

        @Override
        protected void describeMismatchSafely(BindingSet item, Description mismatchDescription) {
            Binding binding = item.getBinding(name);
            if (binding == null) {
                mismatchDescription.appendText("but did not contain such a binding");
                return;
            }
            mismatchDescription.appendText("instead it was bound to").appendValue(binding.getValue());
        }

        @Override
        protected boolean matchesSafely(BindingSet item) {
            Binding binding = item.getBinding(name);
            if (binding == null) {
                return false;
            }
            return valueMatcher.matches(binding.getValue());
        }
    }

    /**
     * Checks that a name isn't bound.
     */
    private static class NotBindsMatcher extends TypeSafeMatcher<BindingSet> {
        /**
         * Name of the binding to check.
         */
        private final String name;

        public NotBindsMatcher(String name) {
            this.name = name;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("does not contain a binding for").appendValue(name);
        }

        @Override
        protected void describeMismatchSafely(BindingSet item, Description mismatchDescription) {
            Binding binding = item.getBinding(name);
            mismatchDescription.appendText("instead it was bound to").appendValue(binding.getValue());
        }

        @Override
        protected boolean matchesSafely(BindingSet item) {
            return item.getBinding(name) == null;
        }
    }

    private Matchers() {
        // Utility constructor
    }
}
