package org.wikidata.query.rdf.tool.change;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.is;

import java.util.function.Function;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

/**
 * Hamcrest matchers for Change class.
 */
public final class ChangeMatchers {
    public static Matcher<Change> hasTitle(String qid) {
        return new ChangeMatcher<String>("title", change -> change.entityId(),
                is(equalTo(qid)));
    }

    @SuppressWarnings("boxing")
    public static Matcher<Change> hasRevision(long revid) {
        return new ChangeMatcher<Long>("revision", change -> change.revision(),
                is(equalTo(revid)));
    }

    @SuppressWarnings("boxing")
    public static Matcher<Change> hasRevision(Matcher<Long> matcher) {
        return new ChangeMatcher<Long>("revision", change -> change.revision(), matcher);
    }

    public static Matcher<Change> hasTitleRevision(String qid, long revid) {
        return both(hasTitle(qid)).and(hasRevision(revid));
    }

    private static class ChangeMatcher<T> extends TypeSafeMatcher<Change> {
        private final Matcher<T> partMatcher;
        private String partName;
        private Function<Change, T> partExtractor;

        ChangeMatcher(String partName, Function<Change, T> partExtractor, Matcher<T> partMatcher) {
            this.partName = partName;
            this.partExtractor = partExtractor;
            this.partMatcher = partMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("Change with " + partName + " that ").appendDescriptionOf(partMatcher);
        }

        @Override
        protected boolean matchesSafely(Change item) {
            return partMatcher.matches(partExtractor.apply(item));
        }
    }

    private ChangeMatchers() {
        // Utility ctor
    }
}
