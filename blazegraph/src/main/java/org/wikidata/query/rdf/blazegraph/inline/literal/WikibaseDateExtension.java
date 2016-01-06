package org.wikidata.query.rdf.blazegraph.inline.literal;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.Duration;

import org.openrdf.model.Literal;
import org.openrdf.model.URI;
import org.openrdf.model.vocabulary.XMLSchema;
import org.wikidata.query.rdf.common.WikibaseDate;
import org.wikidata.query.rdf.common.WikibaseDate.ToStringFormat;

import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.IMathOpHandler;
import com.bigdata.rdf.internal.constraints.MathBOp.MathOp;
import com.bigdata.rdf.internal.impl.literal.AbstractLiteralIV;
import com.bigdata.rdf.internal.impl.literal.LiteralExtensionIV;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;

/**
 * This implementation of {@link com.bigdata.rdf.internal.IExtension} implements
 * inlining for literals that represent xsd:dateTime literals. Unlike
 * {@link com.bigdata.rdf.internal.impl.extensions.DateTimeExtension} on which
 * this is based, it stores the literals as time in <strong>seconds</strong>
 * since the epoch. The seconds are encoded as an inline long. Also unlike
 * DateTimeExtension it only supports UTC as the default time zone because UTC
 * is king. This is needed because Wikidata contains dates that who's
 * <strong>milliseconds</strong> since epoch don't fit into a long.
 *
 * @param <V> Blazegraph value to expand. These are usually treated a bit
 *            roughly by Blazegraph - lots of rawtypes
 */
public class WikibaseDateExtension<V extends BigdataValue> extends AbstractMultiTypeExtension<V>
    implements IMathOpHandler {

    /**
     * List of data types this extension can inline.
     */
    private static final List<URI> SUPPORTED_DATA_TYPES = Collections.unmodifiableList(Arrays.asList(
            XMLSchema.DATETIME, XMLSchema.DATE));

    /**
     * Datatype factory cache.
     */
    protected static final DatatypeFactory DATATYPE_FACTORY;
    static {
        try {
            DATATYPE_FACTORY = DatatypeFactory.newInstance();
        } catch (DatatypeConfigurationException e) {
            throw new RuntimeException(e);
        }
    }

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

    /**
     * Check whether this URI is of the type we support.
     * @param lit
     * @return
     */
    private boolean isWikibaseDateURI(URI lit) {
        if (lit == null) {
            return false;
        }
        if (SUPPORTED_DATA_TYPES.contains(lit)) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("rawtypes")
    @Override
    public boolean canInvokeMathOp(final Literal... args) {
        if (args.length != 2) {
            // for now we handle only two args
            return false;
        }
        URI dt1 = args[0].getDatatype();
        URI dt2 = args[1].getDatatype();

        boolean d1 = isWikibaseDateURI(dt1);
        boolean d2 = isWikibaseDateURI(dt2);

        if (d1 && d2) {
            // both dates, we can handle it
            return true;
        }

        if (d1 && dt2.equals(XMLSchema.DURATION)) {
            // date and duration, is OK
            return true;
        }

        if (d2 && dt1.equals(XMLSchema.DURATION)) {
            // date and duration, is OK
            return true;
        }

        return false;
    }

    /**
     * Normalize IV - convert to LiteralExtension.
     * @param l Original literal, will be parsed if IV is not inlined.
     * @param iv Original IV
     * @return Normalized IV, parsed through Wikidata if needed
     */
    @SuppressWarnings({"rawtypes", "checkstyle:cyclomaticcomplexity"})
    private LiteralExtensionIV normalizeIV(Literal l, IV iv) {
        if (iv instanceof LiteralExtensionIV) {
            return (LiteralExtensionIV)iv;
        } else {
            return createIV(l);
        }
    }

    @SuppressWarnings({"rawtypes", "checkstyle:cyclomaticcomplexity"})
    @Override
    public IV doMathOp(
            final Literal l1, final IV iv1,
            final Literal l2, final IV iv2,
            final MathOp op,
            final BigdataValueFactory vf) {

        URI dt1 = l1.getDatatype();
        URI dt2 = l2.getDatatype();

        boolean d1 = isWikibaseDateURI(dt1);
        boolean d2 = isWikibaseDateURI(dt2);

        if (!d1 && !d2) {
            throw new SparqlTypeErrorException();
        }

        LiteralExtensionIV liv1 = d1 ? normalizeIV(l1, iv1) : null;
        LiteralExtensionIV liv2 = d2 ? normalizeIV(l2, iv2) : null;

        if (d1 && d2) {
            return handleTwoDates(liv1, liv2, op, vf);
        }

        // Now we have one date and one duration
        if (op == MathOp.PLUS) {
            LiteralExtensionIV iv = d1 ? liv1 : liv2;
            Literal lduration = d1 ? l2 : l1;

            return datePlusDuration(iv, DATATYPE_FACTORY.newDuration(lduration.getLabel()));
        }

        if (op == MathOp.MINUS) {
            return datePlusDuration(liv1,
                    DATATYPE_FACTORY.newDuration(l2.getLabel()).negate());
        }

        throw new SparqlTypeErrorException();
    }

    /**
     * Combine two dates.
     * @param iv1
     * @param iv2
     * @param op
     * @param vf
     * @return
     */
    @SuppressWarnings("rawtypes")
    private IV handleTwoDates(
            final LiteralExtensionIV iv1,
            final LiteralExtensionIV iv2,
            final MathOp op,
            final BigdataValueFactory vf) {
        long ts1 = iv1.getDelegate().longValue();
        long ts2 = iv2.getDelegate().longValue();
        switch (op) {
        case MIN:
            return ts1 < ts2 ? iv1 : iv2;
        case MAX:
            return ts1 > ts2 ? iv1 : iv2;
        case MINUS:
            double days = (double) (ts1 - ts2) / ((double) (60 * 60 * 24));
            return new XSDNumericIV(days);
        default:
            throw new SparqlTypeErrorException();
        }
    }

    /**
     * Add Duration to date.
     * @param iv
     * @param d
     * @return
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private IV datePlusDuration(LiteralExtensionIV iv, Duration d) {
        long ts = iv.getDelegate().longValue();
        WikibaseDate newdate = WikibaseDate.fromSecondsSinceEpoch(ts).addDuration(d);
        return new LiteralExtensionIV(new XSDNumericIV(newdate.secondsSinceEpoch()), iv.getExtensionIV());
    }
}
