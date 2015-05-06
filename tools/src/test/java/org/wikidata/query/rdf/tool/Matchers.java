package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.tool.StatementHelper.uri;

import java.util.ArrayList;
import java.util.List;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;
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
            value = WikibaseUris.WIKIDATA.property(PropertyType.CLAIM) + value;
        }
        return new BindsMatcher<URI>(name, equalTo(uri(value)));
    }

    /**
     * Check a binding to a value.
     */
    public static <V> Matcher<BindingSet> binds(String name, V value) {
        return new BindsMatcher<V>(name, equalTo(value));
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

    private Matchers() {
        // Utility constructor
    }
}
