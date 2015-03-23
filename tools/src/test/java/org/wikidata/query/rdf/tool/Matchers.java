package org.wikidata.query.rdf.tool;

import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.tool.StatementHelper.uri;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.openrdf.model.URI;
import org.openrdf.query.Binding;
import org.openrdf.query.BindingSet;

public class Matchers {
    /**
     * Check a binding to a uri.
     */
    public static Matcher<BindingSet> binds(String name, String value) {
        return new BindsMatcher<URI>(name, equalTo(uri(value)));
    }

    public static <V> Matcher<BindingSet> binds(String name, V value) {
        return new BindsMatcher<V>(name, equalTo(value));
    }

    public static class BindsMatcher<V> extends TypeSafeMatcher<BindingSet> {
        private final String name;
        private final Matcher<V> valueMatcher;

        public BindsMatcher(String name, Matcher<V> valueMatcher) {
            this.name = name;
            this.valueMatcher = valueMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("contains a binding from").appendValue(name).appendText("to").appendDescriptionOf(valueMatcher);
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
}
