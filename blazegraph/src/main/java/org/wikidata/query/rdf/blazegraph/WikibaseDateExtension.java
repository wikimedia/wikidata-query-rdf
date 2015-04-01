package org.wikidata.query.rdf.blazegraph;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.log4j.Logger;
import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.util.InnerCause;

/**
 * This implementation of {@link IExtension} implements inlining for literals
 * that represent xsd:dateTime literals. Unlike
 * {@link com.bigdata.rdf.internal.impl.extensions.DateTimeExtension} on which
 * this is based, it stores the literals as time in <strong>seconds</strong>
 * since the epoch. The seconds are encoded as an inline long. Also unlike
 * DateTimeExtension it only supports UTC as the default time zone because UTC
 * is king. This is needed because Wikidata contains dates that who's
 * <strong>milliseconds</strong> since epoch don't fit into a long.
 */
public class WikibaseDateExtension<V extends BigdataValue> implements IExtension<V> {
    private static final Logger log = Logger.getLogger(WikibaseDateExtension.class);

    private static final List<URI> SUPPORTED_DATA_TYPES = Collections.unmodifiableList(Arrays.asList(
            XMLSchema.DATETIME, XMLSchema.DATE));

    @SuppressWarnings("rawtypes")
    private final Map<IV, BigdataURI> dataTypes;
    private final Set<BigdataURI> dataTypesSet;

    public WikibaseDateExtension(final IDatatypeURIResolver resolver) {
        @SuppressWarnings("rawtypes")
        Map<IV, BigdataURI> dataTypes = new HashMap<>();
        for (URI uri : SUPPORTED_DATA_TYPES) {
            BigdataURI val = resolver.resolve(uri);
            dataTypes.put(val.getIV(), val);
        }
        this.dataTypes = Collections.unmodifiableMap(dataTypes);
        dataTypesSet = Collections.unmodifiableSet(new HashSet<>(this.dataTypes.values()));
    }

    @Override
    public Set<BigdataURI> getDatatypes() {
        return dataTypesSet;
    }

    /**
     * Attempts to convert the supplied value into an epoch representation and
     * encodes the long in a delegate {@link XSDNumericIV}, and returns an
     * {@link LiteralExtensionIV} to wrap the native type.
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public LiteralExtensionIV createIV(Value value) {
        if (!(value instanceof Literal)) {
            throw new IllegalArgumentException("Expected a literal but got " + value);
        }
        try {
            Literal literal = (Literal) value;
            BigdataURI dataType = resolveDataType(literal);
            WikibaseDate date = WikibaseDate.fromString(value.stringValue()).cleanWeirdStuff();
            AbstractLiteralIV delegate = new XSDNumericIV(date.secondsSinceEpoch());
            return new LiteralExtensionIV(delegate, dataType.getIV());
        } catch (Exception e) {
            /*
             * Exception logging in blazegraph isn't great for this so we log
             * here as well
             */
            log.warn("Couldn't create IV", e);
            throw e;
        }
    }

    private BigdataURI resolveDataType(Literal literal) {
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

    /**
     * Use the long value of the {@link XSDNumericIV} delegate which represents
     * seconds since the epoch to create a WikibaseDate and then represent that
     * properly using xsd's string representations.
     */
    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public V asValue(final LiteralExtensionIV iv, final BigdataValueFactory vf) {
        if (!dataTypes.containsKey(iv.getExtensionIV())) {
            throw new IllegalArgumentException("Unrecognized datatype:  " + iv.getExtensionIV());
        }

        WikibaseDate date = WikibaseDate.fromSecondsSinceEpoch(iv.getDelegate().longValue());
        try {
            BigdataURI dt = dataTypes.get(iv.getExtensionIV());

            if (dt.equals(XMLSchema.DATE)) {
                return (V) vf.createLiteral(date.toString(ToStringFormat.DATE), dt);
            }

            return (V) vf.createLiteral(date.toString(ToStringFormat.DATE_TIME), dt);
        } catch (RuntimeException ex) {
            if (InnerCause.isInnerCause(ex, InterruptedException.class)) {
                throw ex;
            }
            throw new IllegalArgumentException("bad iv: " + iv, ex);
        }
    }
}
