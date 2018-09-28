package org.wikidata.query.rdf.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.wikidata.query.rdf.common.WikibaseDate.DAYS_PER_MONTH;
import static org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat.DATE;
import static org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat.DATE_TIME;
import static org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat.WIKIDATA;
import static org.wikidata.query.rdf.common.WikibaseDate.fromSecondsSinceEpoch;
import static org.wikidata.query.rdf.common.WikibaseDate.fromString;
import static org.wikidata.query.rdf.common.WikibaseDate.isLeapYear;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import org.joda.time.chrono.GregorianChronology;
import org.junit.Rule;
import org.junit.Test;
import org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat;

import com.carrotsearch.randomizedtesting.annotations.Repeat;

public class WikibaseDateUnitTest {

    @Rule
    public Randomizer randomizer = new Randomizer();

    /**
     * Round trips epoch and explicitly tests some output and input formats.
     */
    @Test
    public void epoch() {
        WikibaseDate wbDate = check(1970, 1, 1, 0, 0, 0);
        assertThat(wbDate.toString(ToStringFormat.WIKIDATA)).isEqualTo("+00000001970-01-01T00:00:00Z");
        assertThat(wbDate.toString(ToStringFormat.DATE_TIME)).isEqualTo("1970-01-01T00:00:00Z");
        assertThat(wbDate.toString(ToStringFormat.DATE)).isEqualTo("1970-01-01");
        assertThat(wbDate).isEqualTo(fromString("1970-1-1"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00Z"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00.123Z"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00+00:00"));
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00-00:00"));
    }

    // Other timezones are not supported yet
    @Test(expected = IllegalArgumentException.class)
    public void badTZ() {
        WikibaseDate wbDate = check(1970, 1, 1, 0, 0, 0);
        assertThat(wbDate).isEqualTo(fromString("1970-1-1T00:00:00-03:00"));
    }

    @Test
    public void yearOne() {
        check(1, 1, 1, 0, 0, 0);
    }

    @Test
    public void yearMinusOne() {
        check(-1, 1, 1, 0, 0, 0);
    }

    @Test
    public void yearZero() {
        check(0, 1, 1, 0, 0, 0);
    }

    @Test
    public void whenIWroteThis() {
        check(2015, 4, 1, 13, 53, 40);
    }

    @Test
    public void onLeapYear() {
        check(2000, 11, 1, 0, 0, 0);
    }

    @Test
    public void negativeLeapYear() {
        check(-4, 11, 1, 0, 0, 0);
    }

    @Test
    public void onLeapYearBeforeLeapDay() {
        check(2000, 2, 28, 13, 53, 40);
    }

    @Test
    public void onLeapYearOnLeapDay() {
        check(2000, 2, 29, 13, 53, 40);
    }

    @Test
    public void onLeapYearAfterLeapDay() {
        check(2000, 3, 1, 13, 53, 40);
    }

    @Test
    public void offLeapYearBeforeLeapDay() {
        check(2001, 2, 28, 13, 53, 40);
    }

    @Test
    public void offLeapYearAfterLeapDay() {
        check(2001, 3, 1, 13, 53, 40);
    }

    @Test
    public void veryNegativeYear() {
        check(-286893830, 1, 1, 0, 0, 0);
    }

    @Test

    public void bigBang() {
        WikibaseDate wbDate = fromString("-13798000000-00-00T00:00:00Z").cleanWeirdStuff();
        assertThat(wbDate).isEqualTo(fromString("-13798000000-01-01T00:00:00Z"));
        assertThat(wbDate.secondsSinceEpoch()).isEqualTo(-435422885863219200L);
        checkRoundTrip(wbDate);
    }

    @Test
    public void transitionCE() {
        long sec1BCE = fromString("-0001-01-01T00:00:00Z").secondsSinceEpoch();
        long sec1CE = fromString("0001-01-01T00:00:00Z").secondsSinceEpoch();
        long daysBetween = (sec1CE - sec1BCE) / (60 * 60 * 24);
        assertThat(daysBetween).isEqualTo(365 + 366);
    }

    @Test
    public void badMonth() {
        WikibaseDate wbDate = fromString("1844-13-31T00:00:00Z");
        wbDate = wbDate.cleanWeirdStuff();
        assertThat(wbDate.year()).isEqualTo(1845);
        assertThat(wbDate.month()).isEqualTo(1);
    }

    @Test
    @Repeat(iterations = 100)
    public void randomDate() {
        // Build a valid random date

        // Joda doesn't work outside these years
        int year = randomizer.randomIntBetween(-292275054, 292278993);
        int month = randomizer.randomIntBetween(1, 12);
        int day;
        if (isLeapYear(year) && month == 2) {
            day = randomizer.randomIntBetween(1, 29);
        } else {
            day = randomizer.randomIntBetween(1, DAYS_PER_MONTH[month - 1]);
        }
        int hour = randomizer.randomIntBetween(0, 23);
        int minute = randomizer.randomIntBetween(0, 59);
        int second = randomizer.randomIntBetween(0, 59);
        check(year, month, day, hour, minute, second);
    }

    @Test
    public void durations() throws DatatypeConfigurationException {
        WikibaseDate wbDate = new WikibaseDate(2016, 8, 5, 0, 0, 0);
        Duration d = DatatypeFactory.newInstance().newDuration("P7D");
        WikibaseDate wdDate7days = wbDate.addDuration(d);
        assertThat(wdDate7days.secondsSinceEpoch()).isEqualTo(jodaSeconds(2016, 8, 12, 0, 0, 0));

        wdDate7days = wbDate.addDuration(d.negate());
        assertThat(wdDate7days.secondsSinceEpoch()).isEqualTo(jodaSeconds(2016, 7, 29, 0, 0, 0));
    }

    /**
     * Checks that the dates resolve the same way joda-time resolves dates and
     * that they round trip.
     */
    private WikibaseDate check(int year, int month, int day, int hour, int minute, int second) {
        WikibaseDate wbDate = new WikibaseDate(year, month, day, hour, minute, second);
        assertThat(wbDate.secondsSinceEpoch()).as(wbDate.toString()).isEqualTo(jodaSeconds(year, month, day, hour, minute, second));
        checkRoundTrip(wbDate);
        return wbDate;
    }

    /**
     * Round trips the date through secondsSinceEpoch and all the toString and
     * fromString formats.
     */
    private void checkRoundTrip(WikibaseDate wbDate) {
        long seconds = wbDate.secondsSinceEpoch();
        WikibaseDate roundDate = fromSecondsSinceEpoch(seconds);
        assertThat(wbDate).isEqualTo(roundDate);
        long roundSeconds = roundDate.secondsSinceEpoch();
        assertThat(seconds).isEqualTo(roundSeconds);

        String string = wbDate.toString(WIKIDATA);
        roundDate = fromString(string);
        assertThat(wbDate).isEqualTo(roundDate);
        String roundString = roundDate.toString(WIKIDATA);
        assertThat(string).isEqualTo(roundString);

        string = wbDate.toString(DATE_TIME);
        roundDate = fromString(string);
        assertThat(wbDate).isEqualTo(roundDate);
        roundString = roundDate.toString(DATE_TIME);
        assertThat(string).isEqualTo(roundString);

        string = wbDate.toString(DATE);
        roundDate = fromString(string);
        if (wbDate.hour() == 0 && wbDate.minute() == 0 && wbDate.second() == 0) {
            assertThat(wbDate).isEqualTo(roundDate);
        }
        roundString = roundDate.toString(DATE);
        assertThat(string).isEqualTo(roundString);
    }

    /**
     * Get the seconds since epoch for a time according to Joda-Time.
     */
    private long jodaSeconds(int year, int month, int day, int hour, int minute, int second) {
        return GregorianChronology.getInstanceUTC().getDateTimeMillis(year, month, day, hour, minute, second, 0) / 1000;
    }

}
