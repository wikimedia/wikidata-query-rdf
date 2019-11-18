package org.wikidata.query.rdf.blazegraph.updater;

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
 * This class implements Merging Updater servlet.
 * The purpose of this API is to provide efficient updates, which would:
 *  - remove out of date site links
 *  - remove out of date statements about statements
 *  - remove out of date statements about the entity
 *  - insert new statements
 *  - insert timestamps
 *  - cleanup refs
 *  - cleanup values
 *
 * @author <a href="mailto:igorkim78@gmail.com">Igor Kim</a>
 */
public class MergingUpdaterServlet extends BigdataRDFServletEx {

    private static final Logger log = LoggerFactory.getLogger(MergingUpdaterServlet.class);
    private static final DiskFileItemFactory factory = new DiskFileItemFactory();

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) throws IOException {
        if (!isWritable(getServletContext(), req, resp)) {
            // Service must be writable.
            return;
        }

        if (ServletFileUpload.isMultipartContent(req)) {

            doUpdateWithBody(req, resp);

        } else {

            resp.setStatus(HttpServletResponse.SC_BAD_REQUEST);

        }
    }

    /**
     * UPDATE request with a request body containing the statements to be
     * removed and added as a multi-part mime request.
     */
    private void doUpdateWithBody(final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {

        log.trace("Processing merging update");

        final ServletFileUpload upload = new ServletFileUpload(factory);

        FileItem insertStatements = null;
        FileItem valueSet = null;
        FileItem refSet = null;

        final List<FileItem> items;
        try {

            items = upload.parseRequest(req);

        } catch (FileUploadException ex) {

            throw new IOException(ex);

        }

        try {
            for (FileItem item : items) {
                insertStatements = checkFileItem(item, insertStatements, "insertStatements");
                valueSet = checkFileItem(item, valueSet, "valueSet");
                refSet = checkFileItem(item, refSet, "refSet");
            }

            final String baseURI = req.getRequestURL().toString();

            final String namespace = getNamespace(req);

            try {

                submitApiTask(
                        new MergingUpdateWithBodyTask(req, resp, namespace,
                                ITx.UNISOLATED,
                                baseURI,
                                insertStatements,
                                valueSet,
                                refSet,
                                UrisSchemeFactory.getURISystem()
                                )).get();

            } catch (DatasetNotFoundException | InterruptedException | ExecutionException | TimeoutException t) {
                launderThrowable(
                        t,
                        resp,
                        getErrorMessage(insertStatements,
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

    private FileItem checkFileItem(FileItem item, FileItem prevValue, String fieldName) {
        if (prevValue != null) {
            return prevValue;
        }
        if (fieldName.equals(item.getFieldName())) {
            return item;
        }
        return null;
    }

    private String getErrorMessage(FileItem insertStatements, final String baseURI) {
        return "MERGING-UPDATE-WITH-BODY: baseURI="
                + baseURI
                + (insertStatements == null ? null
                        : ", insertStatements="
                                + insertStatements
                  );
    }

}
