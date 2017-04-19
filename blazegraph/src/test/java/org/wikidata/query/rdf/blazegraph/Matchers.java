package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeVariable;
import static org.wikidata.query.rdf.test.StatementHelper.uri;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.impl.LiteralImpl;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IConstant;
import com.bigdata.rdf.internal.IV;

/**
 * A set of matchers to match Blazegraph bindings.
 */
public final class Matchers {
    /**
     * Match binding with certain variable name and value.
     * @param name
     * @param value
     * @return
     */
    public static Matcher<IBindingSet> binds(String name, String value) {
        return new BindingMatcher<Literal>(name, equalTo(new LiteralImpl(value)));
    }

    /**
     * Match binding with certain variable name and URI value.
     * @param name
     * @param value
     * @return
     */
    public static Matcher<IBindingSet> bindsItem(String name, String id) {
        return new BindingMatcher<URI>(name, equalTo(uri(id)));
    }

    /**
     * Check that variable is not bound.
     * @param name
     * @param value
     * @return
     */
    public static Matcher<IBindingSet> notBinds(String name) {
        return new NotBindsMatcher(name);
    }

    /**
     * Matcher for Blazegraph bindings.
     * @param <V>
     */
    private static class BindingMatcher<V> extends TypeSafeMatcher<IBindingSet> {
        /**
         * Bound name.
         */
        private final String name;
        /**
         * Delegate matcher for the bound value.
         */
        private final Matcher<V> valueMatcher;

        BindingMatcher(String name, Matcher<V> valueMatcher) {
            this.name = name;
            this.valueMatcher = valueMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("contains a binding from").appendValue(name).appendText("to")
                    .appendDescriptionOf(valueMatcher);
        }

        @Override
        protected void describeMismatchSafely(IBindingSet bindings, Description mismatchDescription) {
            IConstant<IV> value = bindings.get(makeVariable(name));
            if (value == null) {
                mismatchDescription.appendText("but did not contain such a binding");
                return;
            }
            mismatchDescription.appendText("instead it was bound to").appendValue(value.get().getValue());
        }

        @Override
        protected boolean matchesSafely(IBindingSet bindings) {
            IConstant<IV> value = bindings.get(makeVariable(name));
            if (value == null) {
                return false;
            }
            return valueMatcher.matches(value.get().getValue());
        }

    }

    /**
     * Checks that a name isn't bound.
     */
    private static class NotBindsMatcher extends TypeSafeMatcher<IBindingSet> {
        /**
         * Name of the binding to check.
         */
        private final String name;

        NotBindsMatcher(String name) {
            this.name = name;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("does not contain a binding for").appendValue(name);
        }

        @Override
        protected void describeMismatchSafely(IBindingSet bindings, Description mismatchDescription) {
            IConstant<IV> binding = bindings.get(makeVariable(name));
            mismatchDescription.appendText("instead it was bound to").appendValue(binding.get().getValue());
        }

        @Override
        protected boolean matchesSafely(IBindingSet bindings) {
            return bindings.get(makeVariable(name)) == null;
        }
    }

    private Matchers() {
        // Utility constructor
    }
}
