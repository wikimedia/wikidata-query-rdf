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
import org.wikidata.query.rdf.blazegraph.entitiesdata.EntityDataUtil;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.UrisScheme;

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
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.relation.accesspath.IElementFilter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

// Checkstyle suppressed due to this class uses much references on Blazegraph internals.
// Also IVs not always might be provided with corresponding generic types.
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
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
    protected final UrisScheme uris;
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

    private static class InsertData {
        private final Collection<BigdataURI> toInsertEntities = new HashSet<>();
        private final Set<ISPO> toInsert = new HashSet<>();
    }
    @SuppressFBWarnings({"PRMC_POSSIBLY_REDUNDANT_METHOD_CALLS", "WOC_WRITE_ONLY_COLLECTION_LOCAL"})
    private void handleMergingUpdate(final LongAdder nmodified, AbstractTripleStore database)
            throws IOException, RDFParseException, RDFHandlerException {

        List<ISPO> timestampStatements = new ArrayList<>();
        Map<Pair<Resource, Resource>, ISPO> insertBnodes = new HashMap<>();
        Map<Pair<Resource, Resource>, ISPO> deleteBnodes = new HashMap<>();

        String id = UUID.randomUUID().toString().replaceAll("-", "");
        log.debug("APPLYING MERGING UPDATE ID = {}", id);
        BigdataURI normalizedValue = database.getValueFactory().createURI(Ontology.Quantity.NORMALIZED);
        BigdataValue timestampValue = database.getValueFactory().createLiteral(Instant.now().toString(), XMLSchema.DATETIME);
        InsertData insertData = prepareInsertTempStore(database, normalizedValue, timestampValue, insertBnodes);
        Set<ISPO> toDelete = new EntityDataUtil(uris, namespace).prepareDeleteTempStore(database, insertData.toInsertEntities,
                timestampStatements, timestampValue, deleteBnodes);
        insertData.toInsert.addAll(timestampStatements);

        doMergingUpdate(id, database, insertData.toInsert, insertBnodes, toDelete, deleteBnodes, nmodified);

        removeObsoleteRefsAndValues(database, normalizedValue.getIV());

        database.commit();

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
            ISPO sourceBnode = sourceBnodes.get(k);
            if (sourceBnode != null) {
                source.remove(sourceBnode);
            }
        });
    }

    private InsertData prepareInsertTempStore(
                AbstractTripleStore database,
                BigdataValue normalized,
                BigdataValue entityTimestamp,
                Map<Pair<Resource, Resource>, ISPO> bnodeSP
    ) throws IOException, RDFParseException, RDFHandlerException {
        BigdataValueFactory vf = database.getValueFactory();
        InsertData insertData = new InsertData();
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
                            insertData.toInsertEntities.add(vf.asValue((URI) subject));
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
                    EntityDataUtil.assignValue(term.getIV(), term);
                }
            }
            int assigned = 0;
            for (BigdataValue term: termsToResolve) {
                if (term.getIV() == null) {
                    term.setIV(resolvedTerms.get(term));
                    assigned++;
                }
            }
            log.debug("Terms added {} resolved {}, assigned {}", termsAdded, resolvedTerms.size(), assigned);
            database.commit();

            for (Statement stmt: handler.getStatements()) {
                // Values here are BigdataValues, as they are provided by BigdataValueFactory of the database
                BigdataResource s = (BigdataResource) stmt.getSubject();
                BigdataURI p = (BigdataURI) stmt.getPredicate();
                BigdataValue o = (BigdataValue) stmt.getObject();
                insertData.toInsert.add(EntityDataUtil.addIfBnode(new SPO(s, p, o, StatementEnum.Explicit), bnodeSP));
            }
        }
        return insertData;
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
