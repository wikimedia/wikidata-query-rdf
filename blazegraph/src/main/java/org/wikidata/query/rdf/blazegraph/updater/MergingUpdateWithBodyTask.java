package org.wikidata.query.rdf.blazegraph.updater;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.LongAdder;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import javax.annotation.concurrent.NotThreadSafe;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.Resource;
import org.openrdf.model.Statement;
import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFFormat;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.openrdf.rio.RDFParser;
import org.openrdf.rio.Rio;
import org.openrdf.rio.helpers.RDFHandlerBase;
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
import com.bigdata.rdf.model.BigdataResource;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.model.StatementEnum;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.AbstractRestApiTask;
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

// Checkstyle suppressed due to this class uses much references on Blazegraph internals.
// Also IVs not always might be provided with corresponding generic types.
@SuppressWarnings({"checkstyle:classfanoutcomplexity", "rawtypes"})
class MergingUpdateWithBodyTask extends AbstractRestApiTask<Void> {

    // Private logger specific to current class is required by Spotbugs configuration,
    // so logging has to be enabled both to MergingUpdateServlet and MergingUpdateWithBodyTask (or package)
    private static final Logger log = LoggerFactory.getLogger(MergingUpdateWithBodyTask.class);

    private final String baseURI;
    private final FileItem insertStatements;
    private final FileItem valueSet;
    private final FileItem refSet;
    /**
     * URI system.
     */
    private final UrisScheme uris;
    private final boolean outputDumps;

    /**
     *
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp used to obtain a mutable connection.
     * @param baseURI
     *            The base URI for the operation.
     */
    MergingUpdateWithBodyTask(final HttpServletRequest req,
            final HttpServletResponse resp,
            final String namespace, final long timestamp,
            final String baseURI,
            final FileItem insertStatements,
            final FileItem valueSet,
            final FileItem refSet,
            final UrisScheme uris
            ) {
        super(req, resp, namespace, timestamp);
        this.baseURI = baseURI;
        this.insertStatements = insertStatements;
        this.valueSet = valueSet;
        this.refSet = refSet;
        this.uris = uris;
        String property = System.getProperty("outputDumps");
        outputDumps = Boolean.TRUE.toString().equals(property);
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public Void call() throws Exception {

        final long begin = System.currentTimeMillis();

        final LongAdder nmodified = new LongAdder();

        BigdataSailRepositoryConnection repoConn = null;
        try {

            repoConn = getConnection();

            handleMergingUpdate(nmodified, repoConn.getTripleStore());

            final long elapsed = System.currentTimeMillis() - begin;

            reportModifiedCount(nmodified.longValue(), elapsed);

            return null;

        } finally {

            if (repoConn != null) {

               repoConn.close();

            }

        }

    }

    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "WOC_WRITE_ONLY_COLLECTION_LOCAL"})
    private void handleMergingUpdate(final LongAdder nmodified, AbstractTripleStore database)
            throws IOException, RDFParseException, RDFHandlerException {

        Collection<BigdataURI> toInsertEntities = new HashSet<>();
        Collection<BigdataValue> allTerms = new HashSet<>();
        List<ISPO> timestampStatements = new ArrayList<>();
        Map<Pair<Resource, Resource>, ISPO> insertBnodes = new HashMap<>();
        Map<Pair<Resource, Resource>, ISPO> deleteBnodes = new HashMap<>();

        String id = UUID.randomUUID().toString().replaceAll("-", "");
        log.debug("APPLYING MERGING UPDATE ID = {}", id);
        BigdataURI normalizedValue = database.getValueFactory().createURI(Ontology.Quantity.NORMALIZED);
        BigdataValue timestampValue = database.getValueFactory().createLiteral(Instant.now().toString(), XMLSchema.DATETIME);
        Set<ISPO> toInsert = prepareInsertTempStore(database, toInsertEntities, allTerms, normalizedValue, timestampValue, insertBnodes);
        Set<ISPO> toDelete = prepareDeleteTempStore(database, toInsertEntities, timestampStatements, timestampValue, deleteBnodes);
        toInsert.addAll(timestampStatements);

        doMergingUpdate(id, database, toInsert, insertBnodes, toDelete, deleteBnodes, nmodified);

        removeObsoleteRefsAndValues(database, normalizedValue.getIV());

        database.commit();

    }

    private static final class ResolvedValues {
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

    private Set<ISPO> prepareDeleteTempStore(
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

    private ArrayList<IV<?, ?>> collectSubjectsToDelete(AbstractTripleStore database,
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
                assignIV(v, iv);
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

    @SuppressWarnings("unchecked")
    // The proper generics for IV is not available, thus suppressing the check
    private void assignValue(IV iv, BigdataValue v) {
        iv.setValue(v);
    }
    private void assignIV(BigdataValue v, IV iv) {
        v.setIV(iv);
    }

    private static final String DUMP_PATH = System.getProperty("java.io.tmpdir");

    private void dumpStatements(String id, String name, Set<ISPO> collection) throws IOException {
        final String data = collection.stream().map(Object::toString).sorted().collect(Collectors.joining("\n"));
        FileUtils.write(getTmpFile(id, name), data, "UTF-8");
    }

    private File getTmpFile(String id, String name) {
        return Paths.get(DUMP_PATH, id + "." + name).toFile();
    }

    @SuppressFBWarnings(value = "PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", justification = "FB warns on calling database.getStatementCount twice")
    private void doMergingUpdate(
            String id,
            AbstractTripleStore database,
            Set<ISPO> toInsert,
            Map<Pair<Resource, Resource>, ISPO> insertBnodes,
            Set<ISPO> toDelete,
            Map<Pair<Resource, Resource>, ISPO> deleteBnodes,
            final LongAdder nmodified
    ) throws IOException {
        // Do merging update
        if (outputDumps) {
            FileUtils.copyInputStreamToFile(insertStatements.getInputStream(), getTmpFile(id, "insertStatements"));
            dumpStatements(id, "toInsert", toInsert);
            dumpStatements(id, "toDelete", toDelete);
        }

        // Copy the temp store into the database
        long collectedToDelete = toDelete.size();
        long collectedToInsert = toInsert.size();

        Set<ISPO> alreadyExisting = new HashSet<>(toDelete);
        removeWithBNodes(toDelete, deleteBnodes, toInsert, insertBnodes);
        long skippedRemovals = collectedToInsert == 0 ? 0 : collectedToDelete - toDelete.size();
        // remove obsolete statements from DB
        long removed = database.removeStatements(toDelete.toArray(new ISPO[0]), toDelete.size());
        nmodified.add(removed);
        // prepare statements to be inserted into DB
        removeWithBNodes(toInsert, insertBnodes, alreadyExisting, deleteBnodes);
        long skippedInserts = collectedToInsert - toInsert.size();
        long oldCnt = database.getStatementCount(true);
        long insertModified = database.addStatements(toInsert.toArray(new ISPO[0]), toInsert.size());
        nmodified.add(insertModified);
        long inserted = database.getStatementCount(true) - oldCnt;
        // Print the statistics of the merge
        if (outputDumps) {
            dumpStatements(id, "toDeleteClean", toDelete);
            dumpStatements(id, "toInsertClean", toInsert);
        }
        log.debug("Processed update, collectedToDelete {} collectedToInsert {} "
                + "skipped removals {} skipped inserts {} removed {} inserted {} insert modified {} inserts saved {}",
                collectedToDelete, collectedToInsert, skippedRemovals, skippedInserts, removed, inserted, insertModified,
                (collectedToInsert - inserted));
    }

    private void removeWithBNodes(
            Set<ISPO> source,
            Map<Pair<Resource, Resource>, ISPO> sourceBnodes,
            Set<ISPO> toRemove,
            Map<Pair<Resource, Resource>, ISPO> removeBnodes
    ) {
        // firstly remove the exact-matching statements
        source.removeAll(toRemove);
        // then iterate over those statements which have to be removed as bnodes
        removeBnodes.keySet().forEach(k -> {
            if (sourceBnodes.containsKey(k)) {
                source.remove(sourceBnodes.get(k));
            }
        });
    }

    private ISPO addIfBnode(ISPO spo, Map<Pair<Resource, Resource>, ISPO> bnodeMap) {
        // May we need a multimap here if (S,P) pair has more than one bnode?
        if (spo.o().isBNode()) {
            bnodeMap.put(Pair.of(spo.getSubject(), spo.getPredicate()), spo);
        }
        return spo;
    }

    private Set<ISPO> prepareInsertTempStore(
                AbstractTripleStore database,
                Collection<BigdataURI> toInsertEntities,
                Collection<BigdataValue> allTerms,
                BigdataValue normalized,
                BigdataValue entityTimestamp,
                Map<Pair<Resource, Resource>, ISPO> bnodeSP
    ) throws IOException, RDFParseException, RDFHandlerException {
        Set<ISPO> toInsert = new HashSet<>();
        BigdataValueFactory vf = database.getValueFactory();
        if (insertStatements != null) {
            List<BigdataValue> termsToResolve = new ArrayList<>();

            final InputStream is = insertStatements.getInputStream();
            final AddStatementHandler handler = new AddStatementHandler() {
                @Override
                public void handleStatement(Statement st) throws RDFHandlerException {
                    Resource subject = st.getSubject();
                    if (subject instanceof URI) {
                        final String  strURI = subject.stringValue();
                        if (uris.isEntityURI(strURI) && !strURI.startsWith(uris.statement())) {
                            toInsertEntities.add(vf.asValue((URI) subject));
                        }
                    }
                    super.handleStatement(st);
                    termsToResolve.add((BigdataValue)st.getSubject());
                    termsToResolve.add((BigdataValue)st.getPredicate());
                    termsToResolve.add((BigdataValue)st.getObject());
                }
            };

            processData(database, is, handler);
            termsToResolve.add(entityTimestamp);
            termsToResolve.add(normalized);
            long termsAdded = database.getLexiconRelation().addTerms(termsToResolve.toArray(
                    new BigdataValue[0]), termsToResolve.size(), /* readOnly */ false);
            Map<BigdataValue, IV> resolvedTerms = new HashMap<>();
            for (BigdataValue term: termsToResolve) {
                if (term.getIV() != null) {
                    resolvedTerms.put(term, term.getIV());
                    assignValue(term.getIV(), term);
                }
            }
            int assigned = 0;
            for (BigdataValue term: termsToResolve) {
                if (term.getIV() == null) {
                    term.setIV(resolvedTerms.get(term));
                    assigned++;
                }
            }
            allTerms.addAll(termsToResolve);
            log.debug("Terms added {} resolved {}, assigned {}", termsAdded, resolvedTerms.size(), assigned);
            database.commit();

            for (Statement stmt: handler.getStatements()) {
                // Values here are BigdataValues, as they are provided by BigdataValueFactory of the database
                BigdataResource s = (BigdataResource) stmt.getSubject();
                BigdataURI p = (BigdataURI) stmt.getPredicate();
                BigdataValue o = (BigdataValue) stmt.getObject();
                toInsert.add(addIfBnode(new SPO(s, p, o, StatementEnum.Explicit), bnodeSP));
            }
        }
        return toInsert;
    }

    private void removeObsoleteRefsAndValues(AbstractTripleStore database, IV normalized)
            throws IOException {
        BigdataValueFactory vf = database.getValueFactory();
        // cleanup refs if provided
        // Delete SPO where S: refSet, FILTER NOT EXISTS { ?someEntity ?someStatementPred ?s .
        // FILTER(?someStatementPred != <Ontology.Quantity.NORMALIZED>) }
        Collection<BigdataValue> refAndValueList = new HashSet<>();
        if (refSet != null) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(this.refSet.getInputStream(), StandardCharsets.UTF_8))) {
                String ref;
                while ((ref = r.readLine()) != null) {
                    refAndValueList.add(vf.asValue(new URIImpl(ref)));
                }
            }
        }
        // cleanup values if provided
        // Delete SPO where S: valueSet, FILTER NOT EXISTS { ?someEntity ?someStatementPred ?s .
        // FILTER(?someStatementPred != <Ontology.Quantity.NORMALIZED>) }
        if (valueSet != null) {
            try (BufferedReader r = new BufferedReader(new InputStreamReader(this.valueSet.getInputStream(), StandardCharsets.UTF_8))) {
                String value;
                while ((value = r.readLine()) != null) {
                    refAndValueList.add(vf.asValue(new URIImpl(value)));
                }
            }
        }
        database.getLexiconRelation().addTerms(refAndValueList.toArray(new BigdataValue[0]),
                refAndValueList.size(), /* readOnly */ true);
        for (BigdataValue v: refAndValueList) {
            if (v.isRealIV()) {
                database.getAccessPath((IV)null, (IV)null, (IV)v.getIV(), new IElementFilter<ISPO>() {
                    private static final long serialVersionUID = -4038518841880048301L;

                    @Override
                    public boolean isValid(Object object) {
                        ISPO ispo = (ISPO) object;
                        IV iv = null;
                        // Need to recheck predicate due to occassionally an IV might be passed in
                        // for example com.bigdata.rdf.internal.impl.uri.VocabURIByteIV
                        if (ispo.getPredicate() instanceof BigdataValue) {
                            iv = ((BigdataValue) ispo.getPredicate()).getIV();
                        } else if (ispo.getPredicate() instanceof IV) {
                            iv = (IV) ispo.getPredicate();
                        }
                        return !normalized.equals(iv);
                    }

                    @Override
                    public boolean canAccept(Object object) {
                        return (object instanceof ISPO) && (((ISPO)object).getPredicate() instanceof BigdataValue);
                    }

                }).removeAll();
            }
        }
    }

    private void collectSubjects(AbstractTripleStore database,
                                 int keyArity, IV entity, IV predicate, Collection<IV<?, ?>> storage, Collection<IV<?, ?>> allReferenced) {
        if (predicate != null) {
            IChunkedOrderedIterator<ISPO> itr = prepareAccessPath(database, keyArity,
                    new Constant<IV>(entity),
                    new Constant<IV>(predicate),
                    Var.var("o"),
                    null,
                    keyArity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC);
            while (itr.hasNext()) {
                IV subId = itr.next().o();
                storage.add(subId);
                collectReferenced(database, keyArity, subId, allReferenced);
            }
        }
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

    private void collectReferenced(AbstractTripleStore database,
            int keyArity, IV entityOrSubId, Collection<IV<?, ?>> result) {
        collectObjects(database, keyArity,
                new Constant<IV>(entityOrSubId),
                result);
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

    private void collectObjects(AbstractTripleStore database, int keyArity,
            BOp s, Collection<IV<?, ?>> result) {
        final IChunkedOrderedIterator<ISPO> itr = prepareAccessPath(
                database, keyArity,
                s,
                Var.var("p"),
                Var.var("o"),
                /*filter*/ null,
                keyArity == 3 ? SPOKeyOrder.SPO : SPOKeyOrder.SPOC);
        while (itr.hasNext()) {
            result.add(itr.next().o());
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

    /**
     * Helper class adds statements to the sail as they are visited by a parser.
     */
    @NotThreadSafe
    private static class AddStatementHandler extends RDFHandlerBase {

        private final List<Statement> statements;

        AddStatementHandler() {
            this.statements = new ArrayList<>();
        }

        @Override
        public void handleStatement(final Statement stmt)
                throws RDFHandlerException {
            statements.add(stmt);
        }

        public List<Statement> getStatements() {
            return statements;
        }
    }

    private void processData(final AbstractTripleStore toInsert,
            final InputStream is,
            final RDFHandler handler) throws RDFParseException, IOException, RDFHandlerException {

        final RDFParser rdfParser = Rio.createParser(RDFFormat.TURTLE);

        rdfParser.setValueFactory(toInsert.getValueFactory());

//            rdfParser.setVerifyData(true);

//            rdfParser.setStopAtFirstError(true);

//            rdfParser.setDatatypeHandling(RDFParser.DatatypeHandling.IGNORE);

        rdfParser.setRDFHandler(handler);

        /*
         * Run the parser, which will cause statements to be inserted.
         */
        rdfParser.parse(is, baseURI);

    }

}
