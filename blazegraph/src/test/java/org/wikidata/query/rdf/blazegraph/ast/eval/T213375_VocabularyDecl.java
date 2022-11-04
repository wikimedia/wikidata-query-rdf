package org.wikidata.query.rdf.blazegraph.ast.eval;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;

import com.bigdata.rdf.vocab.VocabularyDecl;

@SuppressWarnings({"checkstyle:typename"})
public class T213375_VocabularyDecl implements VocabularyDecl {

    private static final URI[] URIS = new URI[]{
        new URIImpl("http://www.wikidata.org/reference/"),
        new URIImpl("http://www.wikidata.org/value/")
    };

    public T213375_VocabularyDecl() {
    }

    public Iterator<URI> values() {

        return Collections.unmodifiableList(Arrays.asList(URIS)).iterator();

    }

}
