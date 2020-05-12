package org.wikidata.query.rdf.blazegraph.constraints;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.error.SparqlTypeErrorException;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;
import com.bigdata.rdf.internal.impl.uri.URIExtensionIV;
import com.bigdata.rdf.model.BigdataURI;

public class InlineVocabEqBOp extends XSDBooleanIVValueExpression {
    private final IV uriPrefix;

    public InlineVocabEqBOp(BOp[] args, IV inlineVocab) {
        super(args, NOANNS);
        this.uriPrefix = inlineVocab;
        if (args.length != 1 || args[0] == null) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected boolean accept(IBindingSet bs) {
        final IV iv = get(0).get(bs);
        // not yet bound
        if (iv == null)
            throw new SparqlTypeErrorException();
        if (!iv.isInline()) {
            return false;
        }

        if (iv instanceof URIExtensionIV) {
            IV<BigdataURI, ?> extensionIV = ((URIExtensionIV<?>) iv).getExtensionIV();
            return uriPrefix.equals(extensionIV);
        }

        return false;
    }
}
