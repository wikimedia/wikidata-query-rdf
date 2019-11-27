package org.wikidata.query.rdf.tool.rdf;

import java.util.ArrayList;
import java.util.List;

import org.openrdf.model.Statement;
import org.openrdf.rio.RDFHandler;
import org.openrdf.rio.RDFHandlerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.OWL;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.SchemaDotOrg;
import org.wikidata.query.rdf.common.uri.UrisScheme;
import org.wikidata.query.rdf.tool.exception.ContainedException;

import com.codahale.metrics.Meter;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Collects statements about entities until it hits the next entity or the
 * end of the file, munges those statements, and then passes them to the
 * next handler. Note that this relies on the order of the data in the file
 * to be like:
 * <ul>
 * <li>http://www.wikidata.org/wiki/Special:EntityData/EntityId ?p ?o .
 * <li>everything about EntityId
 * <li>http://www.wikidata.org/wiki/Special:EntityData/NextEntityId ?p ?o .
 * <li>etc
 * </ul>
 * This is how the files are built so that is OK.
 */
@SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "the unread lastStatement field is used for debugging")
public class EntityMungingRdfHandler implements RDFHandler {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    /**
     * Uris for this instance of wikibase. We match on these.
     */
    private final UrisScheme uris;
    /**
     * Actually munges the entities!
     */
    private final Munger munger;
    /**
     * The place where we sync munged entities.
     */
    private final EntityCountListener entityMetricConsumer;
    /**
     * The statements about the current entity.
     */
    private final List<Statement> statements = new ArrayList<>();
    /**
     * Meter measuring the number of entities we munge in grand load average
     * style.
     */
    private final Meter entitiesMeter = new Meter();
    private final RDFHandler output;
    /**
     * Have we hit any non Special:EntityData statements? Used to make sure
     * we properly pick up the first few statements in every entity.
     */
    private boolean haveNonEntityDataStatements;
    /**
     * The current entity being read. When we hit a new entity we start send
     * the old statements to the munger and then sync them to next.
     */
    private String entityId;

    public EntityMungingRdfHandler(UrisScheme uris, Munger munger, RDFHandler output, EntityCountListener entityMetricConsumer) {
        this.uris = uris;
        this.munger = munger;
        this.output = output;
        this.entityMetricConsumer = entityMetricConsumer;
    }

    @Override
    public void startRDF() throws RDFHandlerException {
        haveNonEntityDataStatements = false;
        output.startRDF();
    }

    @Override
    public void handleNamespace(String prefix, String uri) throws RDFHandlerException {
        // Namespaces go through to the next handler.
        output.handleNamespace(prefix, uri);
    }

    @Override
    public void handleComment(String comment) throws RDFHandlerException {
        // Comments go right through to the next handler.
        output.handleComment(comment);
    }

    @Override
    @SuppressFBWarnings(value = "STT_STRING_PARSING_A_FIELD", justification = "low priority to fix")
    public void handleStatement(Statement statement) throws RDFHandlerException {
        String subject = statement.getSubject().stringValue();
        if (subject.startsWith(uris.entityDataHttps()) || subject.startsWith(uris.entityData())) {
            if (haveNonEntityDataStatements) {
                munge();
            }
            if (statement.getPredicate().stringValue().equals(SchemaDotOrg.ABOUT)) {
                entityId = statement.getObject().stringValue();
                entityId = entityId.substring(entityId.lastIndexOf('/') + 1);
            }
            statements.add(statement);
            return;
        }
        if (subject.equals(Ontology.DUMP)) {
            if (statement.getPredicate().stringValue().equals(SchemaDotOrg.SOFTWARE_VERSION)) {
                munger.setFormatVersion(statement.getObject().stringValue());
            }
            /*
             * Just pipe dump statements strait through.
             */
            output.handleStatement(statement);
            return;
        }
        if (statement.getPredicate().stringValue().equals(OWL.SAME_AS)) {
            // Temporary fix for T100463
            if (haveNonEntityDataStatements) {
                munge();
            }
            entityId = subject.substring(subject.lastIndexOf('/') + 1);
            statements.add(statement);
            haveNonEntityDataStatements = true;
            return;
        }

        haveNonEntityDataStatements = true;
        statements.add(statement);
    }

    @Override
    public void endRDF() throws RDFHandlerException {
        munge();
        output.endRDF();
    }

    /**
     * Munge an entity's worth of RDF and then sync it the the output.
     *
     * @throws RDFHandlerException if there is an error syncing it
     */
    private void munge() throws RDFHandlerException {
        try {
            log.debug("Munging {}", entityId);
            munger.munge(entityId, statements);
            for (Statement statement : statements) {
                output.handleStatement(statement);
            }
            entitiesMeter.mark();
            if (entitiesMeter.getCount() % 10000 == 0) {
                log.info("Processed {} entities at ({}, {}, {})", entitiesMeter.getCount(),
                        (long) entitiesMeter.getOneMinuteRate(), (long) entitiesMeter.getFiveMinuteRate(),
                        (long) entitiesMeter.getFifteenMinuteRate());
            }
            entityMetricConsumer.entitiesProcessed(entitiesMeter.getCount());
        } catch (ContainedException e) {
            log.warn("Error munging {}", entityId, e);
        }
        statements.clear();
        haveNonEntityDataStatements = false;
    }

    @FunctionalInterface
    public interface EntityCountListener {
        void entitiesProcessed(long entities) throws RDFHandlerException;
    }
}
