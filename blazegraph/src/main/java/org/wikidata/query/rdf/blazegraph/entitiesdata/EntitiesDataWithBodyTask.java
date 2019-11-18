package org.wikidata.query.rdf.blazegraph.entitiesdata;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PushbackInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.lang3.tuple.Pair;
import org.openrdf.model.Resource;
import org.openrdf.model.impl.URIImpl;
import org.openrdf.model.vocabulary.XMLSchema;
import org.openrdf.rio.RDFHandlerException;
import org.openrdf.rio.RDFParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.UrisScheme;

import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.model.BigdataStatement;
import com.bigdata.rdf.model.BigdataURI;
import com.bigdata.rdf.model.BigdataValue;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.bigdata.rdf.sail.BigdataSailRepositoryConnection;
import com.bigdata.rdf.sail.webapp.AbstractRestApiTask;
import com.bigdata.rdf.sail.webapp.BigdataServlet;
import com.bigdata.rdf.spo.ISPO;
import com.bigdata.rdf.store.AbstractTripleStore;
import com.bigdata.rdf.store.BigdataStatementIterator;
import com.bigdata.striterator.ChunkedWrappedIterator;

/**
 * This class is designed to provide checks for data related to specified entityIds.
 * Response includes triples for entity, referenced and linked data, with internal IVs for each component.
 * @author igorkim78@gmail.com
 *
 */
//Checkstyle suppressed due to this class uses much references on Blazegraph internals.
//Also IVs not always might be provided with corresponding generic types.
@SuppressWarnings({"checkstyle:classfanoutcomplexity"})
class EntitiesDataWithBodyTask extends AbstractRestApiTask<Void> {

    private static final Logger log = LoggerFactory.getLogger(EntitiesDataWithBodyTask.class);

    private final FileItem entityIds;

    /**
     * URI system.
     */
    protected final UrisScheme uris;

    /**
     *
     * @param namespace
     *            The namespace of the target KB instance.
     * @param timestamp
     *            The timestamp used to obtain a mutable connection.
     * @param baseURI
     *            The base URI for the operation.
     * @param entityIds
     *            The IDs of entitied which are being checked
     */
    EntitiesDataWithBodyTask(final HttpServletRequest req,
            final HttpServletResponse resp,
            final String namespace, final long timestamp,
            final String baseURI,
            final FileItem entityIds,
            final UrisScheme uris
            ) {
        super(req, resp, namespace, timestamp);
        this.entityIds = entityIds;
        this.uris = uris;
    }

    @Override
    public boolean isReadOnly() {
        return true;
    }

    @Override
    public Void call() throws Exception {

        BigdataSailRepositoryConnection repoConn = null;
        try {

            repoConn = getConnection();

            handleEntitiesCheck(repoConn.getTripleStore());

            return null;

        } finally {

            if (repoConn != null) {

               repoConn.close();

            }

        }

    }

    private void handleEntitiesCheck(AbstractTripleStore database)
            throws IOException, RDFParseException, RDFHandlerException {

        List<ISPO> timestampStatements = new ArrayList<>();
        Map<Pair<Resource, Resource>, ISPO> deleteBnodes = new HashMap<>();

        log.debug("CHECKING ENTITIES");
        BigdataURI normalizedValue = database.getValueFactory().createURI(Ontology.Quantity.NORMALIZED);
        BigdataValue timestampValue = database.getValueFactory().createLiteral(Instant.now().toString(), XMLSchema.DATETIME);
        Collection<BigdataURI> toInsertEntities = prepareCheckTempStore(database, normalizedValue, timestampValue);
        Set<ISPO> toDelete = new EntityDataUtil(uris, namespace).prepareDeleteTempStore(database, toInsertEntities,
                timestampStatements, timestampValue, deleteBnodes);

        writeResponse(database, toDelete);

        long collected = toDelete.size();

        log.debug("Processed update check, collected {}",
                collected);

    }

    private void writeResponse(AbstractTripleStore database, Set<ISPO> toDelete) throws IOException {
        resp.setStatus(HttpServletResponse.SC_OK);
        resp.setContentType(BigdataServlet.MIME_TEXT_PLAIN);
        try (OutputStream os = new BufferedOutputStream(getOutputStream())) {
            final BigdataStatementIterator itr = database
                    .asStatementIterator(new ChunkedWrappedIterator<>(toDelete.iterator()));
            while (itr.hasNext()) {
                BigdataStatement r = itr.next();
                os.write((r.toString() + " "
                        + r.getSubject().getIV() + " "
                        + r.getPredicate().getIV() + " "
                        + r.getObject().getIV() + "\n").getBytes(StandardCharsets.UTF_8));
            }
        }
    }

    private Collection<BigdataURI> prepareCheckTempStore(
                AbstractTripleStore database,
                BigdataValue normalized,
                BigdataValue entityTimestamp
    ) throws IOException, RDFParseException, RDFHandlerException {
        final Collection<BigdataURI> toInsertEntities = new HashSet<>();
        BigdataValueFactory vf = database.getValueFactory();
        if (entityIds != null) {
            List<BigdataValue> termsToResolve = new ArrayList<>();

            try (InputStream is = this.entityIds.getInputStream();
                 InputStreamReader isr = new InputStreamReader(decompressStream(is), StandardCharsets.UTF_8);
                 BufferedReader r = new BufferedReader(isr)) {
                    String entityId;
                    while ((entityId = r.readLine()) != null) {
                        toInsertEntities.add(vf.asValue(new URIImpl(uris.entityIdToURI(entityId))));
                    }
            }

            termsToResolve.add(entityTimestamp);
            termsToResolve.add(normalized);
            long termsAdded = database.getLexiconRelation().addTerms(termsToResolve.toArray(
                    new BigdataValue[0]), termsToResolve.size(), /* readOnly */ true);
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

        }
        return toInsertEntities;
    }

    public static InputStream decompressStream(InputStream input) throws IOException {
        PushbackInputStream pis = new PushbackInputStream(input, 2);
        byte[] signature = new byte[2];
        int len = pis.read(signature); // read the signature
        pis.unread(signature, 0, len); // push back the signature to the stream
        // check if matches standard gzip magic number
        if (signature[0] == (byte) 0x1f && signature[1] == (byte) 0x8b) {
            return new GZIPInputStream(pis);
        } else {
            return pis;
        }
    }
}
