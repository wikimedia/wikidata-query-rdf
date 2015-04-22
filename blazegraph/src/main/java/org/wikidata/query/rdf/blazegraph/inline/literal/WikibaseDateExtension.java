package org.wikidata.query.rdf.blazegraph.inline.literal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat;

import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

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
public class WikibaseDateExtension<V extends BigdataValue> extends AbstractMultiTypeExtension<V> {
    private static final List<URI> SUPPORTED_DATA_TYPES = Collections.unmodifiableList(Arrays.asList(
            XMLSchema.DATETIME, XMLSchema.DATE));

    public WikibaseDateExtension(final IDatatypeURIResolver resolver) {
        super(resolver, SUPPORTED_DATA_TYPES);
    }

    /**
     * Attempts to convert the supplied value into an epoch representation and
     * encodes the long in a delegate {@link XSDNumericIV}, and returns an
     * {@link LiteralExtensionIV} to wrap the native type.
     */
    @Override
    @SuppressWarnings("rawtypes")
    protected AbstractLiteralIV createDelegateIV(Literal literal, BigdataURI dt) {
        WikibaseDate date = WikibaseDate.fromString(literal.stringValue()).cleanWeirdStuff();
        return new XSDNumericIV(date.secondsSinceEpoch());
    }

    /**
     * Use the long value of the {@link XSDNumericIV} delegate which represents
     * seconds since the epoch to create a WikibaseDate and then represent that
     * properly using xsd's string representations.
     */
    @Override
    @SuppressWarnings("rawtypes")
    protected BigdataLiteral safeAsValue(LiteralExtensionIV iv, BigdataValueFactory vf, BigdataURI dt) {
        WikibaseDate date = WikibaseDate.fromSecondsSinceEpoch(iv.getDelegate().longValue());
        if (dt.equals(XMLSchema.DATE)) {
            return vf.createLiteral(date.toString(ToStringFormat.DATE), dt);
        }
        return vf.createLiteral(date.toString(ToStringFormat.DATE_TIME), dt);
    }
}
