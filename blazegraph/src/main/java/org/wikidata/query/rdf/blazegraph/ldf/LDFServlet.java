package org.wikidata.query.rdf.blazegraph.ldf;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.linkeddatafragments.servlet.LinkedDataFragmentServlet;

import com.bigdata.rdf.sail.webapp.BigdataRDFServlet;

/**
 * This class implements LDF servlet.
 * It is based on LinkedDataFragmentServlet from http://linkeddatafragments.org/
 * and on https://github.com/hartig/BlazegraphBasedTPFServer/
 */
public class LDFServlet extends BigdataRDFServlet {
    /**
     *
     */
    private static final long serialVersionUID = -9156574015263443551L;

    /**
     * Delegate LDF servlet.
     * It will be doing all the work.
     */
    private LinkedDataFragmentServlet ldfDelegate;
    /**
     * Overridden to create and initialize the delegate {@link Servlet}
     * instances.
     */
    @Override
    public void init() throws ServletException {
        super.init();
        BlazegraphDataSourceType.setContext(getBigdataRDFContext());
        ldfDelegate = new LinkedDataFragmentServlet();
        ldfDelegate.init(getServletConfig());
    }

    @Override
    protected void doGet(final HttpServletRequest req,
            final HttpServletResponse resp) throws ServletException {
        ldfDelegate.doGet(req, resp);
    }
}
