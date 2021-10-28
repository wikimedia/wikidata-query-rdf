package org.wikidata.query.rdf.blazegraph.events;

import static java.util.Arrays.asList;
import static java.util.Collections.list;
import static java.util.stream.Collectors.toMap;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.http.HttpServletRequest;

public class QueryEventGenerator {
    public static final Pattern NS_EXTRACTER = Pattern.compile("^/namespace/([a-zA-Z0-9_]+)/sparql$");
    public static final String QUERY_PARAM = "query";
    public static final String FORMAT_PARAM = "format";
    private static final Set<String> EXCLUDED_PARAMS = Collections.unmodifiableSet(new HashSet<>(asList(QUERY_PARAM, FORMAT_PARAM)));
    private static final Pattern HEADER_VALUES_ESCAPE_PATTERN = Pattern.compile("([\\\\,])");

    private final Supplier<String> uniqueIdGenerator;
    private final Clock clock;
    private final String hostname;
    private final String stream;
    private final Supplier<Double> systemLoadSupplier;

    public QueryEventGenerator(Supplier<String> uniqueIdGenerator,
                               Clock clock,
                               String hostname,
                               String stream,
                               Supplier<Double> systemLoadSupplier) {
        this.uniqueIdGenerator = uniqueIdGenerator;
        this.clock = clock;
        this.hostname = hostname;
        this.stream = stream;
        this.systemLoadSupplier = systemLoadSupplier;
    }

    public Instant instant() {
        return clock.instant();
    }

    private String extractPath(HttpServletRequest request) {
        assert request.getRequestURI().startsWith(request.getContextPath());
        return request.getRequestURI().substring(request.getContextPath().length());
    }

    public boolean hasValidPath(HttpServletRequest request) {
        String path = this.extractPath(request);
        return "/sparql".equals(path) || QueryEventGenerator.NS_EXTRACTER.matcher(path).matches();
    }

    public QueryEvent generateQueryEvent(HttpServletRequest request, int responseStatus, Duration duration,
                                         Instant queryStartTime, String defaultNamespace, int queriesRunningBefore, int queriesRunningAfter) {
        Objects.requireNonNull(defaultNamespace, "defaultNamespace");
        Objects.requireNonNull(duration, "duration");
        EventMetadata metadata = generateEventMetadata(request, queryStartTime);
        EventHttpMetadata httpMetadata = generateHttpMetadata(request, responseStatus);
        String format = request.getParameter("format");
        String query = request.getParameter("query");
        if (query == null) {
            throw new IllegalArgumentException("query parameter not found");
        }
        String namespace = extractBlazegraphNamespace(extractPath(request));
        if (namespace == null) {
            namespace = defaultNamespace;
        }
        EventPerformer performer = null;
        if (request.getRemoteUser() != null) {
            performer = new EventPerformer(request.getRemoteUser());
        }

        return new QueryEvent(metadata,
                httpMetadata,
                hostname,
                namespace,
                query,
                format,
                extractRequestParams(request),
                duration,
                new SystemRuntimeMetrics(queriesRunningBefore, queriesRunningAfter, systemLoadSupplier.get()),
                performer);
    }

    private EventMetadata generateEventMetadata(HttpServletRequest request, Instant queryStartTime) {
        String reqId = request.getHeader("X-Request-Id");
        String id = uniqueIdGenerator.get();
        if (reqId == null) {
            reqId = uniqueIdGenerator.get();
        }
        return new EventMetadata(reqId, id, queryStartTime, request.getServerName(), stream);
    }

    private EventHttpMetadata generateHttpMetadata(HttpServletRequest httpServletRequest, int responseStatusCode) {
        Map<String, String> headers = list(httpServletRequest.getHeaderNames()).stream()
                .collect(toMap(s -> s.toLowerCase(Locale.ROOT),
                        name -> joinWithComma(list(httpServletRequest.getHeaders(name)))));
        // We run after the ClientIpFilter
        return new EventHttpMetadata(httpServletRequest.getMethod(), httpServletRequest.getRemoteAddr(),
                Collections.unmodifiableMap(headers), httpServletRequest.getCookies() != null, responseStatusCode);
    }

    private Map<String, String> extractRequestParams(HttpServletRequest httpServletRequest) {
        return list(httpServletRequest.getParameterNames()).stream()
                .filter(p -> !EXCLUDED_PARAMS.contains(p))
                .collect(toMap(Function.identity(),
                        name -> joinWithComma(asList(httpServletRequest.getParameterValues(name)))));
    }

    private String extractBlazegraphNamespace(String requestUri) {
        Matcher m = NS_EXTRACTER.matcher(requestUri);
        if (m.find()) {
            return m.group(1);
        }
        return null;
    }

    private String joinWithComma(Collection<String> values) {
        return values.stream().map(this::escapeComma).collect(Collectors.joining(","));
    }

    private String escapeComma(String value) {
        return HEADER_VALUES_ESCAPE_PATTERN.matcher(value).replaceAll("\\\\$1");
    }
}
