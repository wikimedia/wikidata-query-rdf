package org.wikidata.query.rdf.blazegraph.vocabulary;

import java.util.Collection;
import java.util.List;

import org.wikidata.query.rdf.common.uri.WikibaseUris;

import com.bigdata.rdf.vocab.BaseVocabularyDecl;
import com.google.common.collect.ImmutableList;

/**
 * Vocabulary containing the URIs from
 * {@linkplain org.wikidata.query.rdf.common.uri.Ontology} that are imported
 * into Blazegraph.
 */
public class WikibaseUrisVocabularyDecl extends BaseVocabularyDecl {

    /**
     * Get the list of URIs we will import.
     * @param uris Wikibase URIs handler
     */
    private static List<String> getUriList(WikibaseUris uris, Collection<String> prefixes) {
        ImmutableList.Builder<String> uriList = ImmutableList.<String>builder();
        uriList.addAll(uris.entityURIs());
        /*
         * Note that these next URI set is required to make
         * WikibaseInlineUriFactory work with
         * IntegerSuffixInlineUriHandler which is required so we can
         * store entities as unsigned integers.
         */
        uris.entityInitials().forEach(s -> uriList.add(uris.entityIdToURI(s)));
        uriList.add(uris.statement());
        uriList.add(uris.reference());
        uriList.add(uris.value());
        for (String p: prefixes) {
            uriList.add(uris.property(p) + "P");
        }
        return uriList.build();
    }

    public WikibaseUrisVocabularyDecl(WikibaseUris uris, Collection<String> prefixes) {
        super(getUriList(uris, prefixes).toArray());
    }
}
