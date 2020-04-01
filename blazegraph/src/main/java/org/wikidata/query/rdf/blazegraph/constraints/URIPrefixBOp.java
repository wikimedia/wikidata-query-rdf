package org.wikidata.query.rdf.blazegraph.constraints;

import org.openrdf.model.Value;

import com.bigdata.bop.BOp;
import com.bigdata.bop.IBindingSet;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.INeedsMaterialization;
import com.bigdata.rdf.internal.constraints.XSDBooleanIVValueExpression;

public class URIPrefixBOp extends XSDBooleanIVValueExpression implements INeedsMaterialization {
    private final String uriPrefix;

    public URIPrefixBOp(BOp[] args, String uriPrefix) {
        super(args, BOp.NOANNS);
        this.uriPrefix = uriPrefix;
        if (args.length != 1 || args[0] == null) {
            throw new IllegalArgumentException();
        }
    }

    @Override
    protected boolean accept(IBindingSet bs) {
        IV iv = this.getAndCheckBound(0, bs);
        if (!iv.isURI()) {
            return false;
        }
        Value val = asValue(iv);
        return val.stringValue().startsWith(uriPrefix);
    }

    @Override
    public Requirement getRequirement() {
        return Requirement.SOMETIMES;
    }
}
