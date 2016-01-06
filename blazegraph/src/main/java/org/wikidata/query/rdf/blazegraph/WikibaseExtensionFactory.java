package org.wikidata.query.rdf.blazegraph;

import java.util.Collection;
import java.util.Iterator;

//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.blazegraph.inline.literal.WikibaseDateExtension;

import com.bigdata.rdf.internal.DefaultExtensionFactory;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;

/**
 * Setup inline value extensions to Blazegraph for Wikidata.
 */
public class WikibaseExtensionFactory extends DefaultExtensionFactory {
    // private static final Logger log = LoggerFactory.getLogger(WikibaseExtensionFactory.class);

    @Override
    @SuppressWarnings("rawtypes")
    protected void _init(IDatatypeURIResolver resolver, ILexiconConfiguration<BigdataValue> config,
            Collection<IExtension> extensions) {
        if (config.isInlineDateTimes()) {
            Iterator<IExtension> extensionsItr = extensions.iterator();
            while (extensionsItr.hasNext()) {
                if (extensionsItr.next() instanceof DateTimeExtension) {
                    extensionsItr.remove();
                }
            }
            extensions.add(new WikibaseDateExtension<BigdataLiteral>(resolver));
            // log.warn("Installed Wikidata date extensions");
        }
    }
}
