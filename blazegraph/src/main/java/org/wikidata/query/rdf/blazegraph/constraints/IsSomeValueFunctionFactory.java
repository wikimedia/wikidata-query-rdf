package org.wikidata.query.rdf.blazegraph.constraints;

import java.util.Arrays;
import java.util.Map;
import java.util.Objects;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Ontology;

import com.bigdata.bop.BOp;
import com.bigdata.bop.BOpContextBase;
import com.bigdata.bop.IValueExpression;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.internal.constraints.IsBNodeBOp;
import com.bigdata.rdf.lexicon.LexiconRelation;
import com.bigdata.rdf.sparql.ast.FunctionRegistry;
import com.bigdata.rdf.sparql.ast.GlobalAnnotations;
import com.bigdata.rdf.sparql.ast.ValueExpressionNode;
import com.bigdata.rdf.sparql.ast.eval.AST2BOpUtility;

public class IsSomeValueFunctionFactory implements FunctionRegistry.Factory {
    public static final URI IS_SOMEVALUE_FUNCTION_URI = new URIImpl(Ontology.NAMESPACE + "isSomeValue");
    private final SomeValueMode mode;
    private final String skolemURIPrefix;

    public IsSomeValueFunctionFactory(SomeValueMode mode, String skolemURIPrefix) {
        this.mode = mode;
        if (mode == SomeValueMode.Skolem) {
            this.skolemURIPrefix = Objects.requireNonNull(skolemURIPrefix);
        } else {
            this.skolemURIPrefix = skolemURIPrefix;
        }
    }

    @Override
    public IValueExpression<? extends IV> create(BOpContextBase context, GlobalAnnotations globals, Map<String, Object> map, ValueExpressionNode... args) {
        FunctionRegistry.checkArgs(args, ValueExpressionNode.class);
        IValueExpression<? extends IV> ve = AST2BOpUtility.toVE(context, globals, args[0]);
        if (mode == SomeValueMode.Blank) {
            return new IsBNodeBOp(ve);
        } else {
            String namespace = globals.lex;
            long timestamp = globals.timestamp;
            LexiconRelation lex = (LexiconRelation)context.getResource(namespace, timestamp);
            IV vocabPrefix = lex.getContainer().getVocabulary().get(lex.getValueFactory().createURI(skolemURIPrefix));
            if (vocabPrefix == null) {
                throw new IllegalStateException("[" + skolemURIPrefix + "] must be part of the vocabulary");
            }
            return new InlineVocabEqBOp(new BOp[]{ve}, vocabPrefix);
        }
    }

    public enum SomeValueMode {
        Blank,
        Skolem;

        public static SomeValueMode lookup(String value) {
            return Arrays.stream(values())
                    .filter(e -> value.equalsIgnoreCase(e.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("Unsupported SomeValueMode [" + value + "]"));
        }
    }
}
