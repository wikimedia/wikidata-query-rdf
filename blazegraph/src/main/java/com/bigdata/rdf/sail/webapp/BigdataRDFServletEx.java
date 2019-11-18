package com.bigdata.rdf.sail.webapp;

import java.io.IOException;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

// This class inherited from BigdataRDFServlet to get to isWritable method is needed for MergingUpdaterServlet
// but defined in package scope in BigdataServlet
public abstract class BigdataRDFServletEx extends BigdataRDFServlet {

    public static boolean isWritable(final ServletContext servletContext, final HttpServletRequest req,
            final HttpServletResponse resp) throws IOException {
        return BigdataRDFServlet.isWritable(servletContext, req, resp);
    }

}
