package org.wikidata.query.rdf.common.uri;

/**
 * Ontolex ontology URIs.
 * For use in WikibaseLexeme
 */
public final class Ontolex {
    /**
     * ontolex: namespace.
     */
    public static final String NAMESPACE = "http://www.w3.org/ns/lemon/ontolex#";

    /**
     * ontolex:LexicalEntry class.
     * This is the type for Lexeme.
     */
    public static final String LEXICAL_ENTRY = NAMESPACE + "LexicalEntry";
    /**
     * ontolex:Form class.
     */
    public static final String FORM = NAMESPACE + "Form";
    /**
     * ontolex:Sense class.
     */
    public static final String SENSE = NAMESPACE + "Sense";

    /**
     * ontolex:lexicalForm predicate.
     * Links Lexeme to Form.
     */
    public static final String LEXICAL_FORM = NAMESPACE + "lexicalForm";
    /**
     * ontolex:sense predicate.
     * Links Lexeme to Sense.
     */
    public static final String SENSE_PREDICATE = NAMESPACE + "sense";
    /**
     * ontolex:representation predicate.
     * Form textual representation.
     */
    public static final String REPRESENTATION = NAMESPACE + "representation";

    private Ontolex() {
        // Utility uncallable constructor
    }
}
