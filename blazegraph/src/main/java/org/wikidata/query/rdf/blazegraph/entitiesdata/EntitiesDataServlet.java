package org.wikidata.query.rdf.blazegraph.entitiesdata;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.fileupload.FileItem;
import org.apache.commons.fileupload.FileUploadException;
import org.apache.commons.fileupload.disk.DiskFileItemFactory;
import org.apache.commons.fileupload.servlet.ServletFileUpload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

import com.bigdata.journal.ITx;
import com.bigdata.rdf.sail.webapp.BigdataRDFServletEx;
import com.bigdata.rdf.sail.webapp.DatasetNotFoundException;

/**
 * This class implements Entities Data servlet.
 * The purpose of this API is to provide low level analysis,
 * which would for a given list of entity IDs:
 *  - collect all site links and references
 *  - collect statements about statements
 *  - collect statements about the entity
 *  - return collected statements with corresponding IVs.
 *
 *  This servlet provides HTTP GET method to make it clear it does not apply
 *  any changes to the data, but entity IDs might not fit in query params,
 *  so GET request with body is used, which is OK as long we are using
 *  this servlet internally (and most probably having local access to the server)
 *
 * @author <a href="mailto:igorkim78@gmail.com">Igor Kim</a>
 */
public class EntitiesDataServlet extends BigdataRDFServletEx {

    private static final Logger log = LoggerFactory.getLogger(EntitiesDataServlet.class);
    private static final DiskFileItemFactory factory = new DiskFileItemFactory();
    private static final String ENTITY_IDS_PARAM = "entityIds";

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {

        if (ServletFileUpload.isMultipartContent(req)) {

            doEntitiesDataWithBody(req, resp);

        } else {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }
    }

    /**
     * Entities data request with a request body containing the entityIds to be
     * checked (returned from the DB) as a multi-part mime request.
     */
    private void doEntitiesDataWithBody(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        log.trace("Processing entities check");

        final ServletFileUpload upload = new ServletFileUpload(factory);

        final List<FileItem> items;
        try {

            items = upload.parseRequest(req);

        } catch (FileUploadException ex) {

            throw new IOException(ex);

        }

        try {
            final String baseURI = req.getRequestURL().toString();

            FileItem entityIds = getEntityIds(items);
            if (entityIds == null) {
                resp.sendError(HttpServletResponse.SC_BAD_REQUEST,
                        getErrorMessage(entityIds, baseURI));
                return;
            }

            final String namespace = getNamespace(req);

            try {

                submitApiTask(
                        new EntitiesDataWithBodyTask(req, resp, namespace,
                                ITx.UNISOLATED,
                                baseURI,
                                entityIds,
                                UrisSchemeFactory.getURISystem()
                                )).get();

            } catch (DatasetNotFoundException | InterruptedException | ExecutionException | TimeoutException t) {
                launderThrowable(
                        t,
                        resp,
                        getErrorMessage(entityIds,
                                baseURI));
            }
        } finally {
            // release tmp files as soon as they are processed, otherwise it will be delayed until GC
            // and might result in tmp files left behind on disk in case if VM stopped
            for (FileItem item: items) {
                item.delete();
            }
        }
    }

    private FileItem getEntityIds(List<FileItem> items) {
        for (FileItem item : items) {
            if (ENTITY_IDS_PARAM.equals(item.getFieldName())) {
                return item;
            }
        }
        return null;
    }

    private String getErrorMessage(FileItem entityIds, final String baseURI) {
        return "ENTITIES-DATA-WITH-BODY: baseURI="
                + baseURI
                + (entityIds == null ? " No entityIds provided"
                        : ", entityIds="
                                + entityIds
                  );
    }

}
