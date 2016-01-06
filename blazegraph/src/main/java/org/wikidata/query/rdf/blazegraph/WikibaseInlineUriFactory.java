package org.wikidata.query.rdf.blazegraph;

import org.wikidata.query.rdf.blazegraph.inline.uri.UndecoratedUuidInlineUriHandler;
import org.wikidata.query.rdf.common.uri.CommonValues;
import org.wikidata.query.rdf.common.uri.WikibaseUris;
import org.wikidata.query.rdf.common.uri.WikibaseUris.PropertyType;

import com.bigdata.rdf.internal.InlineURIFactory;
import com.bigdata.rdf.internal.InlineURIHandler;
import com.bigdata.rdf.internal.InlineUnsignedIntegerURIHandler;
import com.bigdata.rdf.internal.NormalizingInlineUriHandler;
import com.bigdata.rdf.internal.TrailingSlashRemovingInlineUriHandler;

/**
 * Factory building InlineURIHandlers for wikidata.
 *
 * One thing to consider when working on these is that its way better for write
 * (and probably update) performance if all the bits of an entity are grouped
 * together in Blazegraph's BTrees. Scattering them causes updates to have to
 * touch lots of BTree nodes. {s,p,o}, {p,o,s}, and {o,s,p} are the indexes so
 * and {s,p,o} seems most sensitive to scattering.
 *
 * Another thing to consider is that un-inlined uris are stored as longs which
 * take up 9 bytes including the flags byte. And inlined uris are stored as 1
 * flag byte, 1 (or 2) uri prefix bytes, and then delegate date type. That means
 * that if the delegate data type is any larger than 6 bytes then its a net loss
 * on index size using it. So you should avoid longs and uuids. Maybe even
 * forbid them entirely.
 */
public class WikibaseInlineUriFactory extends InlineURIFactory {
    public WikibaseInlineUriFactory() {
        // TODO lookup wikibase host and default to wikidata
        final WikibaseUris uris = WikibaseUris.WIKIDATA;

        /*
         * Order matters here because some of these are prefixes of each other.
         */
        for (PropertyType p: PropertyType.values()) {
            addHandler(new InlineUnsignedIntegerURIHandler(uris.property(p) + "P"));
        }
        /*
         * We don't use WikibaseStyleStatementInlineUriHandler because it makes
         * things worse!
         */
        addHandler(new InlineUnsignedIntegerURIHandler(uris.entity() + "P"));
        addHandler(new InlineUnsignedIntegerURIHandler(uris.entity() + "Q"));

        // These aren't part of wikibase but are common in wikidata
        // TODO: add more prefixes?
        InlineURIHandler viaf = new TrailingSlashRemovingInlineUriHandler(
                new InlineUnsignedIntegerURIHandler(CommonValues.VIAF));
        addHandler(viaf);
        addHandler(new NormalizingInlineUriHandler(viaf, CommonValues.VIAF_HTTP));

        /*
         * Value nodes are inlined even though they are pretty big (uuids). It
         * doesn't seem to effect performance either way.
         *
         * Statements can't be inlined without losing information or making them
         * huge and bloating the index. We could probably rewrite them at the
         * munger into something less-uuid-ish.
         *
         * References aren't uuids - they are sha1s or sha0s or something
         * similarly 160 bit wide. 160 bits is too big to fit into a uuid so we
         * can't inline that without bloating either.
         */
        addHandler(new UndecoratedUuidInlineUriHandler(uris.value()));
        // addHandler(new UndecoratedUuidInlineUriHandler(uris.reference()));
    }
}
