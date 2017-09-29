package org.wikidata.query.rdf.common;

import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MINUTES;

import java.util.Arrays;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.datatype.Duration;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Handles wikidata dates. Note that this ignores leap seconds. This isn't ok
 * but its what joda time does so it where we're starting.
 */
@SuppressFBWarnings(value = "PL_PARALLEL_LISTS", justification = "SECONDS_PER_MONTH and SECONDS_PER_MONTH_CUMULATIVE make sense as separate arrays")
public class WikibaseDate {
//    private static final transient Logger log = LoggerFactory.getLogger(WikibaseDate.class);

    /**
     * Pattern used to recognize dates sent from wikibase.
     */
    private static final Pattern FORMAT_PATTERN = Pattern
            .compile("(?<year>[+-]?0+)-(?<month>0?0)-(?<day>0?0)(?:T(?<hour>0?0):(?<minute>0?0)(?::(?<second>0?0)(?<ms>[.]000)?)?)?Z?"
                    .replace("0", "\\d"));

    /**
     * Build a WikibaseDate from the string representation. Supported:
     * <ul>
     * <li>+YYYYYYYYYYYY-MM-DDThh:mm:ssZ (Wikidata's default)
     * <li>YYYY-MM-DDThh:mm:ssZ (xsd:dateTime)
     * <li>YYYY-MM-DD (xsd:date with time assumed to be 00:00:00)
     * <li>
     * </ul>
     */
    public static WikibaseDate fromString(String string) {
        // TODO timezones
        Matcher m = FORMAT_PATTERN.matcher(string);
        if (!m.matches()) {
            throw new IllegalArgumentException("Invalid date format:  " + string);
        }
        long year = parseLong(m.group("year"));
        // TODO two digit years without leading zeros might mean something other
        // than the year 20.
        int month = parseInt(m.group("month"));
        int day = parseInt(m.group("day"));
        int hour = parseOr0(m, "hour");
        int minute = parseOr0(m, "minute");
        int second = parseOr0(m, "second");
        // log.debug("Parsed {} into {}-{}-{}T{}:{}:{}", string, year, month, day, hour, minute, second);
        return new WikibaseDate(year, month, day, hour, minute, second);
    }

    /**
     * Parse a group to an int or return 0 if the group wasn't matched.
     */
    private static int parseOr0(Matcher m, String group) {
        String matched = m.group(group);
        if (matched == null) {
            return 0;
        }
        return parseInt(matched);
    }

    /**
     * Build a WikibaseDate from seconds since epoch.
     */
    public static WikibaseDate fromSecondsSinceEpoch(long secondsSinceEpoch) {
        long year = yearFromSecondsSinceEpoch(secondsSinceEpoch);
        int second = (int) (secondsSinceEpoch - calculateFirstDayOfYear(year) * SECONDS_PER_DAY);
        int month = 1;
        long[] secondsPerMonthCumulative = secondsPerMonthCumulative(year);
        while (month < 12 && second >= secondsPerMonthCumulative[month]) {
            month++;
        }
        second -= secondsPerMonthCumulative[month - 1];
        int day = second / SECONDS_PER_DAY + 1;
        second %= SECONDS_PER_DAY;
        int hour = second / SECONDS_PER_HOUR;
        second %= SECONDS_PER_HOUR;
        int minute = second / SECONDS_PER_MINUTE;
        second %= SECONDS_PER_MINUTE;
        // log.debug("Parsed {} into {}-{}-{}T{}:{}:{}", secondsSinceEpoch, year, month, day, hour, minute, second);
        return new WikibaseDate(year, month, day, hour, minute, second);
    }

    /**
     * Number of days from 0 to 1970. Used to find the first day of the year.
     */
    private static final int DAYS_0000_TO_1970 = 719527;
    /**
     * Seconds in a minute. Used when converting to and from seconds since
     * epoch.
     */
    private static final int SECONDS_PER_MINUTE = (int) MINUTES.toSeconds(1);
    /**
     * Seconds in an hour. Used when converting to and from seconds since epoch.
     */
    private static final int SECONDS_PER_HOUR = (int) HOURS.toSeconds(1);
    /**
     * Seconds in a day. Used when converting to and from seconds since epoch.
     */
    private static final int SECONDS_PER_DAY = (int) DAYS.toSeconds(1);
    /**
     * Average number of seconds in a year. Used to guess at the correct year
     * when converting from seconds since epoch.
     */
    private static final long AVERAGE_SECONDS_PER_YEAR = (SECONDS_PER_DAY * 365 * 3 + SECONDS_PER_DAY * 366) / 4;
    /**
     * Pretty good guess at the number of seconds between the start of year 0
     * and epoch. Used to guess at the correct year when converting from seconds
     * since epoch.
     */
    private static final long SECONDS_AT_EPOCH = 1970 * AVERAGE_SECONDS_PER_YEAR;
    /**
     * Days per month in non-leap-years.
     */
    static final int[] DAYS_PER_MONTH = new int[] {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    /**
     * Seconds per month in a year.
     */
    private static final long[] SECONDS_PER_MONTH = new long[12];
    /**
     * Cumulative seconds per month in a non-leap year. Used to convert from
     * seconds since epoch.
     */
    private static final long[] SECONDS_PER_MONTH_CUMULATIVE = new long[12];
    /**
     * Cumulative seconds per month in a leap year. Used to convert from seconds
     * since epoch.
     */
    private static final long[] SECONDS_PER_MONTH_CUMULATIVE_LEAP_YEAR;
    static {
        long total = 0;
        for (int i = 0; i < DAYS_PER_MONTH.length; i++) {
            SECONDS_PER_MONTH[i] = DAYS.toSeconds(DAYS_PER_MONTH[i]);
            SECONDS_PER_MONTH_CUMULATIVE[i] = total;
            total += SECONDS_PER_MONTH[i];
        }
        SECONDS_PER_MONTH_CUMULATIVE_LEAP_YEAR = Arrays.copyOf(SECONDS_PER_MONTH_CUMULATIVE,
                SECONDS_PER_MONTH_CUMULATIVE.length);
        for (int i = 2; i < SECONDS_PER_MONTH_CUMULATIVE_LEAP_YEAR.length; i++) {
            SECONDS_PER_MONTH_CUMULATIVE_LEAP_YEAR[i] += SECONDS_PER_DAY;
        }
    }

    /**
     * Year since epoch.
     */
    private final long year;
    /**
     * Month of the year.
     */
    private final int month;
    /**
     * Day of the month.
     */
    private final int day;
    /**
     * Hour of the day.
     */
    private final int hour;
    /**
     * Minute of the hour.
     */
    private final int minute;
    /**
     * Second of the minute.
     */
    private final int second;

    /**
     * Build using explicit date parts.
     */
    public WikibaseDate(long year, int month, int day, int hour, int minute, int second) {
        this.year = year;
        this.month = month;
        this.day = day;
        this.hour = hour;
        this.minute = minute;
        this.second = second;
    }

    /**
     * Wikidata contains some odd dates like -13798000000-00-00T00:00:00Z and
     * February 30th. We simply guess what they mean here. Note that we really
     * can't be sure what was really ment so these just amount to a best effort.
     *
     * @return this if the date is fine, a new date if we modified it
     */
    public WikibaseDate cleanWeirdStuff() {
        long newYear = year;
        int newMonth = month;
        int newDay = day;
        if (month == 0) {
            newMonth = 1;
        }
        if (month > 12) {
            newYear += (month - 1) / 12;
            newMonth = (month - 1) % 12 + 1;
        }
        if (day == 0) {
            newDay = 1;
        } else {
            int maxDaysInMonth = daysInMonth(newYear, newMonth);
            if (newDay > maxDaysInMonth) {
                newMonth++;
                newDay = newDay - maxDaysInMonth + 1;
                if (newMonth > 12) {
                    newMonth = newMonth - 12;
                    newYear++;
                }
            }
        }
        if (newYear == year && newMonth == month && newDay == day) {
            return this;
        }
        return new WikibaseDate(newYear, newMonth, newDay, hour, minute, second);
    }

    /**
     * Convert this date into a number of seconds since epoch.
     */
    @SuppressFBWarnings(value = "ICAST_INTEGER_MULTIPLY_CAST_TO_LONG", justification = "No risk of overflow here")
    public long secondsSinceEpoch() {
        long seconds = calculateFirstDayOfYear(year) * SECONDS_PER_DAY;
        seconds += SECONDS_PER_MONTH_CUMULATIVE[month - 1];
        seconds += (day - 1) * SECONDS_PER_DAY;
        seconds += hour * SECONDS_PER_HOUR;
        seconds += minute * SECONDS_PER_MINUTE;
        seconds += second;
        if (month > 2 && isLeapYear(year)) {
            seconds += SECONDS_PER_DAY;
        }
        return seconds;
    }

    /**
     * Build a WikibaseDate from the string representation. See ToStringFormat
     * for more.
     */
    public String toString(ToStringFormat format) {
        return format.format(this);
    }

    @Override
    public String toString() {
        return toString(ToStringFormat.WIKIDATA);
    }

    /**
     * Year component of the date.
     */
    public long year() {
        return year;
    }

    /**
     * Month component of the date.
     */
    public int month() {
        return month;
    }

    /**
     * Day component of the date.
     */
    public int day() {
        return day;
    }

    /**
     * Hour component of the date.
     */
    public int hour() {
        return hour;
    }

    /**
     * Minute component of the date.
     */
    public int minute() {
        return minute;
    }

    /**
     * Second component of the date.
     */
    public int second() {
        return second;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + day;
        result = prime * result + hour;
        result = prime * result + minute;
        result = prime * result + month;
        result = prime * result + second;
        return prime * result + (int) (year ^ (year >>> 32));
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        WikibaseDate other = (WikibaseDate) obj;
        if (day != other.day) {
            return false;
        }
        if (hour != other.hour) {
            return false;
        }
        if (minute != other.minute) {
            return false;
        }
        if (month != other.month) {
            return false;
        }
        if (second != other.second) {
            return false;
        }
        if (year != other.year) {
            return false;
        }
        return true;
    }

    /**
     * Format for toString.
     */
    public enum ToStringFormat {
        /**
         * Wikidata style (+YYYYYYYYYYYY-MM-DDThh:mm:ssZ).
         */
        WIKIDATA {
            @Override
            public String format(WikibaseDate date) {
                return String.format(Locale.ROOT, "%+012d-%02d-%02dT%02d:%02d:%02dZ", date.year, date.month, date.day,
                        date.hour, date.minute, date.second);
            }
        },
        /**
         * xsd:dateTime style (YYYY-MM-DDThh:mm:ssZ).
         */
        DATE_TIME {
            @Override
            public String format(WikibaseDate date) {
                return String.format(Locale.ROOT, "%04d-%02d-%02dT%02d:%02d:%02dZ", date.year, date.month, date.day,
                        date.hour, date.minute, date.second);
            }
        },
        /**
         * xsd:date style (YYYY-MM-DD).
         */
        DATE {
            @Override
            public String format(WikibaseDate date) {
                return String.format(Locale.ROOT, "%04d-%02d-%02d", date.year, date.month, date.day);
            }
        };

        /**
         * Format the date in this particular style.
         */
        public abstract String format(WikibaseDate date);
    }

    /**
     * Is the provided year a leap year?
     */
    static boolean isLeapYear(long year) {
        // Borrowed from joda-time's GregorianChronology
        return ((year & 3) == 0) && ((year % 100) != 0 || (year % 400) == 0);
    }

    /**
     * Find the first day of the year.
     */
    static long calculateFirstDayOfYear(long year) {
        /*
         * This is a clever hack for getting the number of leap years that works
         * properly for negative years borrowed from JodaTime's
         * GregorianChronology.
         */
        long leapYears = year / 100;
        if (year < 0) {
            leapYears = ((year + 3) >> 2) - leapYears + ((leapYears + 3) >> 2) - 1;
        } else {
            leapYears = (year >> 2) - leapYears + (leapYears >> 2);
            if (isLeapYear(year)) {
                leapYears--;
            }
        }
        return year * 365L + leapYears - DAYS_0000_TO_1970;
    }

    /**
     * Find the year from the number of seconds since epoch.
     */
    static long yearFromSecondsSinceEpoch(long secondsSinceEpoch) {
        /*
         * Similar to Joda-Time's way of getting year from date - estimate and
         * then fix the estimate. Except our estimates can be really off.
         */
        long unitSeconds = AVERAGE_SECONDS_PER_YEAR / 2;
        long i2 = secondsSinceEpoch / 2 + SECONDS_AT_EPOCH / 2;
        if (i2 < 0) {
            i2 = i2 - unitSeconds + 1;
        }
        long year = i2 / unitSeconds;
        /*
         * Here is where we diverge from Joda-Time because our estimates can be
         * off by more than one. Rather than doing something smart we just walk
         * year by year until we get one that fits. So far this looks to be fast
         * enough.
         */
        while (true) {
            // TODO Rerunning calculateFirstDayOfYear looks inefficient
            long yearStart = calculateFirstDayOfYear(year) * SECONDS_PER_DAY;
            long diff = secondsSinceEpoch - yearStart;
            if (diff < 0) {
                year--;
                continue;
            }
            if (diff >= SECONDS_PER_DAY * 365) {
                yearStart += SECONDS_PER_DAY * 365;
                if (isLeapYear(year)) {
                    yearStart += SECONDS_PER_DAY;
                }
                if (yearStart <= secondsSinceEpoch) {
                    year++;
                    continue;
                }
            }
            return year;
        }
    }

    /**
     * Get the number of seconds per month cumulative in a particular year.
     */
    static long[] secondsPerMonthCumulative(long year) {
        if (isLeapYear(year)) {
            return SECONDS_PER_MONTH_CUMULATIVE_LEAP_YEAR;
        }
        return SECONDS_PER_MONTH_CUMULATIVE;
    }

    /**
     * The number of days in a month.
     */
    private static int daysInMonth(long year, int month) {
        int d = DAYS_PER_MONTH[month - 1];
        if (month == 2 && isLeapYear(year)) {
            d++;
        }
        return d;
    }

    /**
     * Add Duration to time value.
     * @param d
     */
    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:npathcomplexity"})
    public WikibaseDate addDuration(Duration d) {
        int sign = d.getSign();
        int newSecond = second + sign * d.getSeconds();
        int newMinute = minute + sign * d.getMinutes();
        int newHour = hour + sign * d.getHours();
        int newDay = day + sign * d.getDays();
        int newMonth = (month - 1) + sign * d.getMonths();
        long newYear = year + sign * d.getYears();

        if (newSecond < 0) {
            int mins = newSecond / SECONDS_PER_MINUTE - 1;
            newSecond -= SECONDS_PER_MINUTE * mins;
            newMinute += mins;
        } else if (newSecond >= SECONDS_PER_MINUTE) {
            int mins = newSecond / SECONDS_PER_MINUTE;
            newSecond -= mins * SECONDS_PER_MINUTE;
            newMinute += mins;
        }

        if (newMinute < 0) {
            int hrs = newMinute / 60 - 1;
            newMinute -= 60 * hrs;
            newHour += hrs;
        } else if (newMinute >= 60) {
            int hrs = newMinute / 60;
            newMinute -= hrs * 60;
            newHour += hrs;
        }

        if (newHour < 0) {
            int days = newHour / 24 - 1;
            newMinute -= 60 * days;
            newDay += days;
        } else if (newHour >= 24) {
            int days = newHour / 24;
            newHour -= days * 24;
            newDay += days;
        }

        // To simplify calculations, we temporarily uses months 0-11
        if (newMonth < 0) {
            int yr = newMonth / 12 - 1;
            newMonth -= 12 * yr;
            newYear += yr;
        } else if (newMonth >= 12) {
            int yr = newMonth / 12;
            newMonth -= yr * 12;
            newYear += yr;
        }
        // Add 1 back to month
        newMonth++;

        if (newDay <= 0) {
            while (newDay <= 0) {
                newMonth--;
                if (newMonth == 0) {
                    newYear--;
                    newMonth = 12;
                }
                newDay += daysInMonth(newYear, newMonth);
            }
        } else if (newDay > daysInMonth(newYear, newMonth)) {
            int dim;
            for (dim = daysInMonth(newYear, newMonth); newDay > dim; dim = daysInMonth(newYear, newMonth)) {
                newDay -= dim;
                newMonth++;
                if (newMonth > 12) {
                    newYear++;
                    newMonth = 1;
                }
            }
        }

        return new WikibaseDate(newYear, newMonth, newDay, newHour, newMinute, newSecond);
    }
}
