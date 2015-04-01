package org.wikidata.query.rdf.blazegraph;

import java.util.Collection;
import java.util.Iterator;

import com.bigdata.rdf.internal.DefaultExtensionFactory;
import com.bigdata.rdf.internal.IDatatypeURIResolver;
import com.bigdata.rdf.internal.IExtension;
import com.bigdata.rdf.internal.ILexiconConfiguration;
import com.bigdata.rdf.internal.impl.extensions.DateTimeExtension;
import com.bigdata.rdf.model.BigdataLiteral;
import com.bigdata.rdf.model.BigdataValue;

public class WikibaseExtensionFactory extends DefaultExtensionFactory {
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
        }
    }
}
