package org.wikidata.query.rdf.blazegraph;

import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasProperty;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.instanceOf;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.Test;
import org.wikidata.query.rdf.common.uri.CommonValues;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.TermId;
import com.bigdata.rdf.internal.impl.literal.XSDIntegerIV;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataValue;

public class WikibaseInlineUriFactoryUnitTest extends AbstractRandomizedBlazegraphTestBase {
    @Test
    public void entityAndTruthyAreInlined() {
        BigdataStatement statement = roundTrip("entity:Q23", "truthy:P509", "entity:Q356405");
        assertThat(statement.getSubject().getIV(), uriIv(uris().entity() + "Q", "23"));
        assertThat(statement.getPredicate().getIV(), uriIv(uris().truthy() + "P", "509"));
        assertThat(statement.getObject().getIV(), uriIv(uris().entity() + "Q", "356405"));
    }

    @Test
    public void valueIsInlined() {
        BigdataStatement statement = roundTrip("s:Q23-01EDEEEE-F0DF-4A07-980F-5E76866B74D7", "v:P1711", 100686);
        assertThat(statement.getSubject().getIV(), instanceOf(TermId.class));
        assertThat(statement.getPredicate().getIV(), uriIv(uris().value() + "P", "1711"));
        assertThat(statement.getObject().getIV(), instanceOf(XSDIntegerIV.class));
    }

    @Test
    public void expandedValuesAreInlined() {
        BigdataStatement statement = roundTrip("s:Q23-01EDEEEE-F0DF-4A07-980F-5E76866B74D7", "v:P580-value",
                "v:91212dc3fcc8b65607d27f92b36e5761");
        assertThat(statement.getSubject().getIV(), instanceOf(TermId.class));
        assertThat(statement.getPredicate().getIV(), uriIv(uris().value() + "P", "580-value"));
        assertThat(statement.getObject().getIV(), uriIv(uris().value(), "91212dc3fcc8b65607d27f92b36e5761"));
    }

    @Test
    public void qualifiersAreInlined() {
        BigdataStatement statement = roundTrip("s:Q23-01EDEEEE-F0DF-4A07-980F-5E76866B74D7", "q:P1711", 100686);
        assertThat(statement.getSubject().getIV(), instanceOf(TermId.class));
        assertThat(statement.getPredicate().getIV(), uriIv(uris().qualifier() + "P", "1711"));
        assertThat(statement.getObject().getIV(), instanceOf(XSDIntegerIV.class));
    }

    @Test
    public void qualifierValuesAreInlined() {
        BigdataStatement statement = roundTrip("s:Q23-01EDEEEE-F0DF-4A07-980F-5E76866B74D7", "q:P1711-value", 100686);
        assertThat(statement.getSubject().getIV(), instanceOf(TermId.class));
        assertThat(statement.getPredicate().getIV(), uriIv(uris().qualifier() + "P", "1711-value"));
        assertThat(statement.getObject().getIV(), instanceOf(XSDIntegerIV.class));
    }

    @Test
    public void viafIsInlined() {
        BigdataStatement statement = roundTrip(CommonValues.VIAF + "123313", CommonValues.VIAF_HTTP + "1234555",
                CommonValues.VIAF_HTTP + "23466/");
        assertThat(statement.getSubject().getIV(), uriIv(CommonValues.VIAF, "123313"));
        assertThat(statement.getPredicate().getIV(), uriIv(CommonValues.VIAF, "1234555"));
        assertThat(statement.getObject().getIV(), uriIv(CommonValues.VIAF, "23466"));
    }

    @SuppressWarnings("rawtypes")
    public static Matcher<IV> uriIv(String namespace, String localName) {
        /*
         * Note that we can't test the localName property of the IV because that
         * contains the un-inflated delegate which is boring. So we test the
         * full name instead.
         */
        String fullName = namespace + localName;
        return allOf(//
                instanceOf((Class<? extends IV>) URIExtensionIV.class),//
                hasProperty("namespace", equalTo(namespace)),//
                hasValue(fullName));
    }

    @SuppressWarnings("rawtypes")
    public static Matcher<IV> hasValue(String string) {
        return new HasValueMatcher(hasToString(string));
    }

    @SuppressWarnings("rawtypes")
    private static class HasValueMatcher extends TypeSafeMatcher<IV> {
        private final Matcher<? super BigdataValue> valueMatcher;

        public HasValueMatcher(Matcher<? super BigdataValue> valueMatcher) {
            this.valueMatcher = valueMatcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText(" value to match ").appendDescriptionOf(valueMatcher);
        }

        @Override
        protected void describeMismatchSafely(IV item, Description mismatchDescription) {
            mismatchDescription.appendText("was ").appendValue(item.getValue());
        }

        @Override
        protected boolean matchesSafely(IV item) {
            return valueMatcher.matches(item.getValue());
        }
    }
}
