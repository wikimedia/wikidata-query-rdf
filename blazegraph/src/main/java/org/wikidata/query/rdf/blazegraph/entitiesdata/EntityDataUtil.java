package org.wikidata.query.rdf.blazegraph.entitiesdata;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontolex;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;

import com.bigdata.bop.BOp;
import com.bigdata.bop.Constant;
import com.bigdata.bop.IPredicate;
import com.bigdata.bop.NV;
import com.bigdata.bop.Var;
import com.bigdata.bop.ap.Predicate;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.spo.SPO;
import com.bigdata.rdf.spo.SPOKeyOrder;
import com.bigdata.rdf.spo.SPOPredicate;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.ElementFilter;
import com.bigdata.relation.accesspath.IElementFilter;
import com.bigdata.striterator.IChunkedOrderedIterator;
import com.bigdata.striterator.IKeyOrder;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public class EntityDataUtil {

    private static final Logger log = LoggerFactory.getLogger(EntityDataUtil.class);
    private final UrisScheme uris;
    private final String namespace;

    public EntityDataUtil(UrisScheme uris, String namespace) {
        this.uris = uris;
        this.namespace = namespace;
    }

    public String getNamespace() {
        return namespace;
    }

    @SuppressWarnings("unchecked")
    // The proper generics for IV is not available, thus suppressing the check
    public static void assignValue(IV iv, BigdataValue v) {
        iv.setValue(v);
    }

    public static void assignIV(BigdataValue v, IV iv) {
        v.setIV(iv);
    }

    public Set<ISPO> prepareDeleteTempStore(
            AbstractTripleStore database,
            Collection<BigdataURI> toInsertEntities,
            List<ISPO> timestampStatements,
            BigdataValue timestampValue,
            Map<Pair<Resource, Resource>, ISPO> deleteBnodes
    ) {
        // Collect out of date statements to be removed from the DB

        int incomingEntitiesCount = toInsertEntities.size();
        List<IV> entityIVs = new ArrayList<>(incomingEntitiesCount);

        ResolvedValues resolvedValues = resolveValues(database, toInsertEntities, incomingEntitiesCount,
                entityIVs, timestampValue);

        ArrayList<IV<?, ?>> subjs = collectSubjectsToDelete(database, timestampStatements, entityIVs, resolvedValues);
        return collectStatementsToDelete(database, deleteBnodes, subjs);
    }

    static final class ResolvedValues {
        final IV schemaAbout;
        final IV ontolexLexicalForm;
        final IV ontolexSense;
        final IV wikibaseTimestamp;
        final IV entityTimestamp;
        ResolvedValues(
                final IV schemaAbout,
                final IV ontolexLexicalForm,
                final IV ontolexSense,
                final IV wikibaseTimestamp,
                final IV entityTimestamp) {
            this.schemaAbout = schemaAbout;
            this.ontolexLexicalForm = ontolexLexicalForm;
            this.ontolexSense = ontolexSense;
            this.wikibaseTimestamp = wikibaseTimestamp;
            this.entityTimestamp = entityTimestamp;
        }
    }

    private ResolvedValues resolveValues(AbstractTripleStore database, Collection<BigdataURI> toInsertEntities,
            int incomingEntitiesCount, List<IV> entityIVs, BigdataValue timestampValue) {
        // Resolve values against target DB
        List<BigdataValue> valuesToResolve = new ArrayList<>(toInsertEntities);

        BigdataValueFactory vf = database.getValueFactory();
        // adding some uris to resolve along with entities
        Stream.of(
            SchemaDotOrg.ABOUT,
            Ontolex.LEXICAL_FORM,
            Ontolex.SENSE,
            Ontology.TIMESTAMP
        ).map(uri -> vf.asValue(new URIImpl(uri))).forEach(valuesToResolve::add);

        BigdataValue[] resolvedValues = valuesToResolve.toArray(new BigdataValue[0]);
        database.getLexiconRelation().addTerms(resolvedValues, resolvedValues.length, /* readOnly */ true);
        int i = 0;
        while (i < incomingEntitiesCount) {
            BigdataValue v = resolvedValues[i];
            if (v.isRealIV()) {
                IV iv = v.getIV();
                EntityDataUtil.assignIV(v, iv);
                entityIVs.add(iv);
            }
            i++;
        }
        IV schemaAbout = resolvedValues[i++].getIV();
        IV ontolexLexicalForm = resolvedValues[i++].getIV();
        IV ontolexSense = resolvedValues[i++].getIV();
        IV wikibaseTimestamp = resolvedValues[i++].getIV();
        IV entityTimestamp = timestampValue.getIV();
        // This must match the order of URIs above!
        log.debug("Using schemaAbout {} ontolexLexicalForm {} ontolexSense {}",
                schemaAbout, ontolexLexicalForm, ontolexSense);

        log.debug("Processing update, running potential deletes query entityIVs {}", entityIVs.size());
        // Note that this method returns only a few IVs, but there is also as a side effect:
        // IVs are assigned to passed in toInsertEntities
        return new ResolvedValues(schemaAbout, ontolexLexicalForm, ontolexSense, wikibaseTimestamp, entityTimestamp);
    }

    @SuppressFBWarnings(
            value = "OCP_OVERLY_CONCRETE_PARAMETER",
            justification = "timestampStatements needs to be a List, order is important here")
    protected ArrayList<IV<?, ?>> collectSubjectsToDelete(AbstractTripleStore database,
            List<ISPO> timestampStatements, List<IV> entityIVs, ResolvedValues resolvedValues) {
        Collection<IV<?, ?>> subjSiteLinks = new HashSet<>();
        Collection<IV<?, ?>> subjStatements = new HashSet<>();
        Collection<IV<?, ?>> subjEntity = new HashSet<>();
        int keyArity = database.getSPOKeyArity();
        for (IV entity: entityIVs) {
            // Collect out of date site links subjects
            collectSiteLinks(database, keyArity, resolvedValues.schemaAbout, entity, subjSiteLinks);

            // Collect out of date about and statements subjects
            collectEntityAndStatement(database, keyArity, resolvedValues.ontolexLexicalForm, resolvedValues.ontolexSense,
                entity, subjStatements, subjEntity);
            // Add timestamps
            // ?entity wikibase:timestamp %ts% .
            timestampStatements.add(new SPO(entity, resolvedValues.wikibaseTimestamp, resolvedValues.entityTimestamp, StatementEnum.Explicit));
        }

        // Collect all db statements related to the subjects of the change, which might become out of date
        ArrayList<IV<?, ?>> subjs = new ArrayList<>(subjEntity);
        subjs.addAll(subjSiteLinks);
        subjs.addAll(subjStatements);
        return subjs;
    }

    private Set<ISPO> collectStatementsToDelete(AbstractTripleStore database,
            Map<Pair<Resource, Resource>, ISPO> deleteBnodes, Iterable<IV<?, ?>> subjs) {
        // TODO replace which straightforward insert of access path for each subject with parallelized read using
        // either Rule or triplePatters read from the db
        Set<ISPO> toDelete = new HashSet<>();
        for (IV<?, ?> s: subjs) {
            IChunkedOrderedIterator<ISPO> itr = database.getAccessPath(s, null, null).iterator();
            while (itr.hasNext()) {
                toDelete.add(addIfBnode(itr.next(), deleteBnodes));
            }
        }
        return toDelete;
    }

    public static ISPO addIfBnode(ISPO spo, Map<Pair<Resource, Resource>, ISPO> bnodeMap) {
        // May we need a multimap here if (S,P) pair has more than one bnode?
        if (spo.o().isBNode()) {
            bnodeMap.put(Pair.of(spo.getSubject(), spo.getPredicate()), spo);
        }
        return spo;
    }

    private void collectEntityAndStatement(AbstractTripleStore database,
            int keyArity, IV ontolexLexicalForm, IV ontolexSense, IV entity,
            Collection<IV<?, ?>> subjStatements,
            Collection<IV<?, ?>> subjEntity) {
        // Collect out of date statements about the entity
        // entityOrSubId: ?entity UNION ?entity (ontolex:ontolexLexicalForm|ontolex:sense) ?entityOrSubId .
        subjEntity.add(entity);
        // Collect out of date statements about statements
        // SPO where S: ?entityOrSubId ?statementPred ?s . FILTER( STRSTARTS(STR(?s), "<uris.statement()>") ) .
        Collection<IV<?, ?>> allReferenced = new HashSet<>();
        collectReferenced(database, keyArity, entity, allReferenced);
        collectSubjects(database, keyArity, entity, ontolexLexicalForm, subjEntity, allReferenced);
        collectSubjects(database, keyArity, entity, ontolexSense, subjEntity, allReferenced);

        // resolve about statements subjects and
        final Map<IV<?, ?>, BigdataValue> terms = database.getLexiconRelation().getTerms(allReferenced,
                4000/* termsChunkSize */, 4000/* blobsChunkSize */);
        for (BigdataValue subj: terms.values()) {
            if (subj.stringValue().startsWith(uris.statement())) {
                subjStatements.add(subj.getIV());
            }
        }
    }

    private void collectSubjects(AbstractTripleStore database, int keyArity, IV entity, IV predicate,
            Collection<IV<?, ?>> storage, Collection<IV<?, ?>> allReferenced) {
        if (predicate != null) {
            IChunkedOrderedIterator<ISPO> itr = prepareAccessPath(database, keyArity, new Constant<IV>(entity),
                    new Constant<IV>(predicate), Var.var("o"), null,
                    keyArity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC);
            while (itr.hasNext()) {
                IV subId = itr.next().o();
                storage.add(subId);
                collectReferenced(database, keyArity, subId, allReferenced);
            }
        }
    }

    private void collectReferenced(AbstractTripleStore database, int keyArity, IV entityOrSubId,
            Collection<IV<?, ?>> result) {
        collectObjects(database, keyArity, new Constant<IV>(entityOrSubId), result);
    }

    private void collectObjects(AbstractTripleStore database, int keyArity, BOp s, Collection<IV<?, ?>> result) {
        final IChunkedOrderedIterator<ISPO> itr = prepareAccessPath(database, keyArity, s, Var.var("p"), Var.var("o"),
                /* filter */ null, keyArity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC);
        while (itr.hasNext()) {
            result.add(itr.next().o());
        }
    }

    private void collectSubjects(AbstractTripleStore database, int keyArity,
            BOp p, BOp o, Collection<IV<?, ?>> result) {
        final IChunkedOrderedIterator<ISPO> itr = prepareAccessPath(
                database,
                keyArity,
                Var.var("s"),
                p,
                o,
                /*filter*/ null,
                keyArity == 3 ? SPOKeyOrder.POS : SPOKeyOrder.POCS);
        while (itr.hasNext()) {
            result.add(itr.next().s());
        }
    }

    private IChunkedOrderedIterator<ISPO> prepareAccessPath(AbstractTripleStore database, int keyArity, BOp s,
            BOp p, BOp o, IElementFilter<ISPO> filter, IKeyOrder<ISPO> keyOrder) {
        Predicate<ISPO> pred = new SPOPredicate(
            keyArity == 4 ? new BOp[]{
                  s,
                  p,
                  o,
                  Var.var("c"),
            } : new BOp[] {
                  s,
                  p,
                  o,
                  },
            NV.asMap(new NV(IPredicate.Annotations.RELATION_NAME,
                    new String[] {getNamespace()}))
        );
        if (filter != null) {
            // Layer on an optional filter.
            pred = pred.addIndexLocalFilter(ElementFilter.newInstance(filter));
        }
        return database.getSPORelation().getAccessPath(
                keyOrder, pred).iterator();
    }

    private void collectSiteLinks(AbstractTripleStore database, int keyArity, IV schemaAbout,
            IV entity, Collection<IV<?, ?>> result) {
        // Clear out of date site links
        // SPO where S: ?s <SchemaDotOrg.ABOUT> ?entity .

        collectSubjects(database, keyArity,
                new Constant<IV>(schemaAbout),
                new Constant<IV>(entity),
                result);
    }

}
