package org.wikidata.query.rdf.blazegraph.inline.literal;

import static java.util.Collections.unmodifiableMap;
import static java.util.Collections.unmodifiableSet;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;

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
 */
public abstract class AbstractMultiTypeExtension<V extends BigdataValue> implements IExtension<V> {
    private static final Logger log = Logger.getLogger(WikibaseDateExtension.class);

    @SuppressWarnings("rawtypes")
    private final Map<IV, BigdataURI> dataTypes;
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
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
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public V asValue(LiteralExtensionIV iv, BigdataValueFactory vf) {
        BigdataURI dt = resolveDataType(iv);
        try {
            return (V) safeAsValue(iv, vf, dt);
        } catch (RuntimeException ex) {
            if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
                throw ex;
            }
            throw new IllegalArgumentException("bad iv: " + iv, ex);
        }
    }

    @SuppressWarnings("rawtypes")
    protected abstract AbstractLiteralIV createDelegateIV(Literal literal, BigdataURI dt);

    @SuppressWarnings("rawtypes")
    protected abstract BigdataLiteral safeAsValue(LiteralExtensionIV iv, BigdataValueFactory vf, BigdataURI dt);

    @SuppressWarnings("rawtypes")
    protected BigdataURI resolveDataType(LiteralExtensionIV literal) {
        BigdataURI dt = dataTypes.get(literal.getExtensionIV());
        if (dt == null) {
            throw new IllegalArgumentException("Unrecognized datatype:  " + literal.getExtensionIV());
        }
        return dt;
    }

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
