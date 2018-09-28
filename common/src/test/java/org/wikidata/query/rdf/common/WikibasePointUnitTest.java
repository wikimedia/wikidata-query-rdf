package org.wikidata.query.rdf.common;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;
import org.wikidata.query.rdf.common.WikibasePoint.CoordinateOrder;

public class WikibasePointUnitTest {

    @Test
    public void fromStringDefault() {
        pointFromString("Point(56.98 12.34)", "12.34", "56.98", null);
    }

    @Test
    public void fromStringLatLong() {
        pointFromStringOrder("Point(12.34 56.98)", "12.34", "56.98", null, CoordinateOrder.LAT_LONG);
    }

    @Test
    public void fromStringLongLat() {
        pointFromStringOrder("Point(12.34 56.98)", "56.98", "12.34", null, CoordinateOrder.LONG_LAT);
    }

    @Test
    public void fromStringGlobe() {
        pointFromString("<On the Moon> Point(56.98 12.34)", "12.34", "56.98", "On the Moon");
    }

    @Test
    public void fromStringGlobeLongLat() {
        pointFromStringOrder("<On Mars> Point(12.34 56.98)", "56.98", "12.34", "On Mars", CoordinateOrder.LONG_LAT);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badFormat() {
        pointFromString("Points(12.34,56.98)", "12.34", "56.98", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badFormat2() {
        pointFromString("On Mars> Point(12.34 56.98)", "12.34", "56.98", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badFormat3() {
        pointFromString("<On Mars>Point(12.34 56.98)", "12.34", "56.98", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badOnlyOne() {
        pointFromString("<On Mars> Point(12.34)", "12.34", "56.98", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badThree() {
        pointFromString("<On Mars> Point(12.34 5.6 7.8)", "12.34", "56.98", null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void badUrl() {
        pointFromString("<On the Moon Point(12.34 56.98)", "12.34", "56.98", "On the Moon");
    }

    private void pointFromString(String s, String lat, String lon, String globe) {
        WikibasePoint p = new WikibasePoint(s);
        assertThat(p.getLatitude()).isEqualTo(lat);
        assertThat(p.getLongitude()).isEqualTo(lon);
        assertThat(p.getGlobe()).isEqualTo(globe);
    }

    private void pointFromStringOrder(String s, String lat, String lon, String globe, CoordinateOrder order) {
        WikibasePoint p = new WikibasePoint(s, order);
        assertThat(p.getLatitude()).isEqualTo(lat);
        assertThat(p.getLongitude()).isEqualTo(lon);
        assertThat(p.getGlobe()).isEqualTo(globe);
    }

    @Test
    public void roundtripTests() {
        roundtrip("Point(12.34 56.98)");
        roundtrip("<http://what?> Point(12.34 56)");
        roundtrip("<not even url> Point(12 56.98)");
    }

    private void roundtrip(String s) {
        WikibasePoint p = new WikibasePoint(s);
        assertThat(s).isEqualTo(p.toString());
    }

    @Test
    public void fromComps() {
        pointFromCompsOrder("Point(12.34 56.98)", "12.34", "56.98", null, CoordinateOrder.LAT_LONG);
        pointFromCompsOrder("Point(56.98 12.34)", "12.34", "56.98", null, CoordinateOrder.LONG_LAT);
        pointFromCompsOrder("<On the Moon> Point(12.34 56.98)", "12.34", "56.98", "On the Moon", CoordinateOrder.LAT_LONG);
        pointFromCompsOrder("<http://mars> Point(56.98 12.34)", "12.34", "56.98", "http://mars", CoordinateOrder.LONG_LAT);
    }

    private void pointFromCompsOrder(String s, String lat, String lon, String globe, CoordinateOrder order) {
        WikibasePoint p = new WikibasePoint(new String[] {lat, lon}, globe, CoordinateOrder.LAT_LONG);
        assertThat(s).isEqualTo(p.toOrder(order));
    }
}
