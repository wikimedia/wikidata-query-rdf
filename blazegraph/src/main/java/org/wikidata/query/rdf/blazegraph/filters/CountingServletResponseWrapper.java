package org.wikidata.query.rdf.blazegraph.filters;

import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;

import javax.servlet.ServletOutputStream;
import javax.servlet.WriteListener;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;

import com.google.common.io.CountingOutputStream;

/**
 * Response wrapper that counts the number of bytes written to the client.
 *
 * The count is captured on the raw byte stream, so it reflects the size of the
 * (uncompressed) payload produced by the servlet. Blazegraph streams SPARQL
 * results through {@link #getOutputStream()}; {@link #getWriter()} is wrapped as
 * well to cover the (typically small) HTML/error paths.
 */
public class CountingServletResponseWrapper extends HttpServletResponseWrapper {
    private CountingOutputStream countingStream;
    private ServletOutputStream outputStream;
    private PrintWriter writer;

    public CountingServletResponseWrapper(HttpServletResponse response) {
        super(response);
    }

    private CountingOutputStream countingStream() throws IOException {
        if (countingStream == null) {
            countingStream = new CountingOutputStream(super.getOutputStream());
        }
        return countingStream;
    }

    @Override
    public ServletOutputStream getOutputStream() throws IOException {
        if (outputStream == null) {
            ServletOutputStream delegate = super.getOutputStream();
            CountingOutputStream counting = countingStream();
            outputStream = new ServletOutputStream() {
                @Override
                public void write(int b) throws IOException {
                    counting.write(b);
                }

                @Override
                public void write(byte[] b, int off, int len) throws IOException {
                    counting.write(b, off, len);
                }

                @Override
                public void flush() throws IOException {
                    counting.flush();
                }

                @Override
                public void close() throws IOException {
                    counting.close();
                }

                @Override
                public boolean isReady() {
                    return delegate.isReady();
                }

                @Override
                public void setWriteListener(WriteListener writeListener) {
                    delegate.setWriteListener(writeListener);
                }
            };
        }
        return outputStream;
    }

    @Override
    public PrintWriter getWriter() throws IOException {
        if (writer == null) {
            writer = new PrintWriter(new OutputStreamWriter(countingStream(), getCharacterEncoding()));
        }
        return writer;
    }

    @Override
    public void flushBuffer() throws IOException {
        if (writer != null) {
            writer.flush();
        }
        super.flushBuffer();
    }

    /**
     * Number of bytes written to the client so far. Flushes the writer (if any)
     * first so buffered characters are accounted for.
     */
    public long getBytesWritten() {
        if (writer != null) {
            writer.flush();
        }
        return countingStream == null ? 0L : countingStream.getCount();
    }
}
