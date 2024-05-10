package org.wikidata.query.rdf.blazegraph.filters;

import static com.google.common.base.MoreObjects.firstNonNull;
import static java.lang.Boolean.parseBoolean;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;

import javax.annotation.Untainted;
import javax.servlet.FilterConfig;

/**
 * Adapter around javax.servlet.FilterConfig that provides:
 * - typed accessor (string, bool, int)
 * - fallback to JVM System properties
 * - default values support
 * for the configuration of the filter.
 */
public class FilterConfiguration {
    public static final String WDQS_CONFIG_PREFIX = "wdqs";
    private final FilterConfig filterConfig;
    private final String systemPropertyPrefix;

    public FilterConfiguration(FilterConfig filterConfig, String systemPropertyPrefix) {
        this.filterConfig = filterConfig;
        this.systemPropertyPrefix = systemPropertyPrefix + "." + filterConfig.getFilterName();
    }
    /**
     * Load a parameter from multiple locations.
     *
     * System properties have the highest priority, filter config is used if no
     * system property is found.
     *
     * The system property used is {@code wdqs.<filter-name>.<name>}.
     *
     * @param name name of the property
     * @return the value of the parameter
     */
    @Untainted // we implicitly trust configuration files
    public String loadStringParam(String name) {
        String result = null;
        String fParam = filterConfig.getInitParameter(name);
        if (fParam != null) {
            result = fParam;
        }
        String sParam = System.getProperty(systemPropertyPrefix + "." + name);
        if (sParam != null) {
            result = sParam;
        }
        return result;
    }

    /**
     * Load a parameter from multiple locations, with a default value.
     *
     * @return the parameter's value
     */
    public String loadStringParam(String name, String defaultValue) {
        return firstNonNull(loadStringParam(name), defaultValue);
    }

    /**
     * Load a parameter from multiple locations, with a default value.
     *
     * @return the parameter's value
     */
    public boolean loadBooleanParam(String name, boolean defaultValue) {
        String result = loadStringParam(name);
        return result != null ? parseBoolean(result) : defaultValue;
    }

    /**
     * Load a parameter from multiple locations, with a default value.
     *
     * @return the parameter's value
     */
    public int loadIntParam(String name, int defaultValue) {
        String result = loadStringParam(name);
        return result != null ? parseInt(result) : defaultValue;
    }

    /**
     * Load a parameter from multiple locations, with a default value.
     *
     * @return the parameter's value
     */
    public double loadDoubleParam(String name, double defaultValue) {
        String result = loadStringParam(name);
        return result != null ? parseDouble(result) : defaultValue;
    }
}
