package org.wikidata.query.rdf.blazegraph.inline.literal;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.util.InnerCause;

/**
 * Abstract base class for implementing IExtension for multiple dataTypes.
 *
 * @param <V> Blazegraph value to expand. These are usually treated a bit
 *            roughly by Blazegraph - lots of rawtypes
 */
public abstract class AbstractMultiTypeExtension<V extends BigdataValue> implements IExtension<V> {
    private static final Logger log = LoggerFactory.getLogger(WikibaseDateExtension.class);

    /**
     * IV to type map as resolved against resolver provided on construction.
     */
    @SuppressWarnings("rawtypes")
    private final Map<IV, BigdataURI> dataTypes;
    /**
     * Set of data type uris resolved on construction.
     */
    private final Set<BigdataURI> dataTypesSet;

    public AbstractMultiTypeExtension(IDatatypeURIResolver resolver, List<URI> supportedDataTypes) {
        @SuppressWarnings("rawtypes")
        Map<IV, BigdataURI> dataTypes = new HashMap<>();
        for (URI uri : supportedDataTypes) {
            BigdataURI val = resolver.resolve(uri);
            dataTypes.put(val.getIV(), val);
        }
        this.dataTypes = unmodifiableMap(dataTypes);
        dataTypesSet = unmodifiableSet(new HashSet<>(this.dataTypes.values()));
    }

    @Override
    public Set<BigdataURI> getDatatypes() {
        return dataTypesSet;
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked", "checkstyle:illegalcatch"})
    public LiteralExtensionIV createIV(Value value) {
        if (!(value instanceof Literal)) {
            throw new IllegalArgumentException("Expected a literal but got " + value);
        }
        Literal literal = (Literal) value;
        try {
            BigdataURI dt = resolveDataType(literal);
            return new LiteralExtensionIV(createDelegateIV(literal, dt), dt.getIV());
        } catch (Exception e) {
            /*
             * Exception logging in blazegraph isn't great for this so we log
             * here as well
             */
            log.warn("Couldn't create IV", e);
            throw e;
        }
    }

    @Override
    @SuppressWarnings({"rawtypes", "unchecked", "checkstyle:illegalcatch"})
    public V asValue(LiteralExtensionIV iv, BigdataValueFactory vf) {
        BigdataURI dt = resolveDataType(iv);
        try {
            return (V) safeAsValue(iv, vf, dt);
        } catch (RuntimeException ex) {
            /*
             * Catching this runtime exception is a Blazegraph idiom that we're
             * just continuing to use.
             */
            if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
                throw ex;
            }
            throw new IllegalArgumentException("bad iv: " + iv, ex);
        }
    }

    /**
     * Create the delegate iv for the literal.
     */
    @SuppressWarnings("rawtypes")
    protected abstract AbstractLiteralIV createDelegateIV(Literal literal, BigdataURI dt);

    /**
     * Convert the iv into the value. Its ok to throw exceptions here and the
     * caller will catch them an make them ok with Blazegraph, thus "safe".
     */
    @SuppressWarnings("rawtypes")
    protected abstract BigdataLiteral safeAsValue(LiteralExtensionIV iv, BigdataValueFactory vf, BigdataURI dt);

    /**
     * Convert the literal into a uri as resolved by the resolve this extension
     * received on construction.
     */
    @SuppressWarnings("rawtypes")
    protected BigdataURI resolveDataType(LiteralExtensionIV literal) {
        BigdataURI dt = dataTypes.get(literal.getExtensionIV());
        if (dt == null) {
            throw new IllegalArgumentException("Unrecognized datatype:  " + literal.getExtensionIV());
        }
        return dt;
    }

    /**
     * Resolve the data type uri from the literal's type.
     */
    protected BigdataURI resolveDataType(Literal literal) {
        URI dt = literal.getDatatype();
        if (dt == null) {
            throw new IllegalArgumentException("Literal doesn't have a data type:  " + literal);
        }

        // TODO why loop instead of use a hash set or something?
        for (BigdataURI val : dataTypes.values()) {
            // Note: URI.stringValue() is efficient....
            if (val.stringValue().equals(dt.stringValue())) {
                return val;
            }
        }
        throw new IllegalArgumentException("Unrecognized data type:  " + dt);
    }
}
