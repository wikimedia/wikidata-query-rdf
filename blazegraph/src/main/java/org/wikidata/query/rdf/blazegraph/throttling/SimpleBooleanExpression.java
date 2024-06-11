package org.wikidata.query.rdf.blazegraph.throttling;

import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import com.google.common.base.Preconditions;

import lombok.AccessLevel;
import lombok.AllArgsConstructor;

/**
 * Provides a simple expression language over boolean values.
 *
 * Keys must start with a letter and contain only letters and -
 * Keys will be evaluated via the provided predicate.
 *
 * Supported operators: && (and), ! (not).
 *
 * Example expressions:
 *   a && !b
 *   a && b
 *   x-custom-header && !x-other-header
 */
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class SimpleBooleanExpression {
    private static final Predicate<String> VALIDATE_KEY = Pattern.compile("^\\w[-\\w]*$").asPredicate();
    private final List<Function<Predicate<String>, Boolean>> expressions;

    public static SimpleBooleanExpression create(String expression) {
        return new SimpleBooleanExpression(Arrays.stream(expression.split("\\s*&&\\s*"))
            .map(SimpleBooleanExpression::parsePart)
            .collect(Collectors.toList()));
    }

    private static Function<Predicate<String>, Boolean> parsePart(String part) {
        boolean isNegated = part.startsWith("!");
        String key = isNegated ? part.substring(1) : part;
        Preconditions.checkArgument(VALIDATE_KEY.test(key), "Invalid key: %s", key);
        return predicate -> isNegated != predicate.test(key);
    }

    public boolean evaluate(Predicate<String> predicate) {
        return expressions.stream().allMatch(fn -> fn.apply(predicate));
    }
}
