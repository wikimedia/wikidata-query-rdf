package org.wikidata.query.rdf.tool.rdf;


import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.openrdf.model.Literal;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.Value;
import org.openrdf.model.impl.LiteralImpl;
import org.openrdf.model.impl.StatementImpl;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.RDF;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;

/**
 * An RDFHandler that wraps another handler normalizing any of the (currently)
 * rather different wikidata output forms into a single form.
 */
public class NormalizingRdfHandler extends DelegatingRdfHandler {
    public NormalizingRdfHandler(RDFHandler next) {
        super(next);
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        if (uri.contains("ontology-0.0.1")) {
            uri = uri.replace("ontology-0.0.1", "ontology");
        }
        if (uri.contains("ontology-beta")) {
            uri = uri.replace("ontology-beta", "ontology");
        }
        super.handleNamespace(prefix, uri);
    }

    @Override
    public void handleStatement(Statement statement) throws RDFHandlerException {
        Resource subject = statement.getSubject();
        URI predicate = statement.getPredicate();
        Value object = statement.getObject();

        if (subject instanceof URI) {
            subject = fixUri((URI) subject);
        }
        predicate = fixUri(predicate);
        if (object instanceof URI) {
            object = fixUri((URI) object);
        } else if (object instanceof Literal) {
            object = fixLiteral((Literal)object);
        }

        // No need to build a new statement if the old one matches.
        if (subject != statement.getSubject() || predicate != statement.getPredicate()
                || object != statement.getObject()) {
            statement = new StatementImpl(subject, predicate, object);
        }
        super.handleStatement(statement);
    }

    /**
     * Check whether the string is a decimal numeric string.
     * @return Whether the string is an acceptable number.
     */
    private boolean isNumericString(final String s) {
        int i = 0;
        if (s.length() == 0) {
            return false;
        }
        final char[] chars = s.toCharArray();
        boolean seenDot = false;

        if (chars[0] == '+' || chars[0] == '-') {
            i++;
        }
        while (i < s.length()) {
            if (chars[i] == '.') {
                if (seenDot) {
                    return false;
                }
                seenDot = true;
            } else  if (chars[i] < '0' || chars[i] > '9') {
                return false;
            }
            i++;
        }
        return true;
    }

    /*
    Bad chars:
    U+00AD  SOFT HYPHEN
    U+200B  ZERO WIDTH SPACE
    U+200E  LEFT-TO-RIGHT MARK
    U+200F  RIGHT-TO-LEFT MARK
    U+202A  LEFT-TO-RIGHT EMBEDDING
    U+202B  RIGHT-TO-LEFT EMBEDDING
    U+202C  POP DIRECTIONAL FORMATTING
    U+202D  LEFT-TO-RIGHT OVERRIDE
    U+202E  RIGHT-TO-LEFT OVERRIDE
    U+2060  WORD JOINER
    U+2061  FUNCTION APPLICATION
    U+2062  INVISIBLE TIMES
    U+2063  INVISIBLE SEPARATOR
    U+2064  INVISIBLE PLUS
    U+206A  INHIBIT SYMMETRIC SWAPPING
    U+206B  ACTIVATE SYMMETRIC SWAPPING
    U+206C  INHIBIT ARABIC FORM SHAPING
    U+206D  ACTIVATE ARABIC FORM SHAPING
    U+206E  NATIONAL DIGIT SHAPES
    U+206F  NOMINAL DIGIT SHAPES
    */
    private final Pattern badchars = Pattern.compile("\u00AD|[\u200B-\u200F]|[\u202A-\u202E]|[\u2060-\u2064]|[\u206A-\u206F]");

    /**
     * Fixes numeric literal by ensuring it is actually contains numeric data.
     * If not, it will be converted to 0.
     */
    private Value fixLiteral(Literal value) {
        URI datatype = value.getDatatype();
        // Numbers - ensure it's numeric
        if (datatype.equals(XMLSchema.DECIMAL)) {
            if (!isNumericString(value.getLabel())) {
                return new LiteralImpl("0", XMLSchema.DECIMAL);
            }
        } else if (datatype.equals(XMLSchema.INTEGER)) {
            if (!isNumericString(value.getLabel())) {
                return new LiteralImpl("0", XMLSchema.INTEGER);
            }
        }
        // Strings - remove "bad" characters
        if (datatype.equals(RDF.LANGSTRING) || datatype.equals(XMLSchema.STRING)) {
            final String strValue = value.getLabel();
            Matcher match = badchars.matcher(strValue);
            if (match.find()) {
                if (value.getLanguage() != null) {
                    return new LiteralImpl(match.replaceAll(""), value.getLanguage());
                } else {
                    return new LiteralImpl(match.replaceAll(""), datatype);
                }
            }
        }
        return value;
    }

    /**
     * Fixes a uri if it contains something unacceptable otherwise just returns
     * the same uri.
     */
    private URI fixUri(URI r) {
        /*
         * Some dumps contained a versioned ontology but those are getting
         * unversioned soon.
         */
        if (r.stringValue().contains("ontology-0.0.1")) {
            r = new URIImpl(r.stringValue().replace("ontology-0.0.1", "ontology"));
        }
        if (r.stringValue().contains("ontology-beta")) {
            r = new URIImpl(r.stringValue().replace("ontology-beta", "ontology"));
        }
        // Temporary bugfix for dump URLs having bad characters in them
        String fixed = StringUtils.replaceEach(r.stringValue(),
                new String[]{"\n", "|",   "\\",  "{",   "}",   "`",   "^"},
                new String[]{"",   "%7C", "%5C", "%7B", "%7D", "%60", "%5E"});
        if (!fixed.equals(r.stringValue())) {
            r = new URIImpl(fixed);
        }
        return r;
    }
}
