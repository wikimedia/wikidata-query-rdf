package org.wikidata.query.rdf.blazegraph.filters;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;

import javax.servlet.ServletOutputStream;

import org.junit.Test;
import org.springframework.mock.web.MockHttpServletResponse;

public class CountingServletResponseWrapperUnitTest {

    @Test
    public void countsBytesWrittenToOutputStream() throws IOException {
        MockHttpServletResponse response = new MockHttpServletResponse();
        CountingServletResponseWrapper wrapper = new CountingServletResponseWrapper(response);

        byte[] payload = "some bytes".getBytes(StandardCharsets.UTF_8);
        ServletOutputStream out = wrapper.getOutputStream();
        out.write(payload);
        out.flush();

        assertThat(wrapper.getBytesWritten()).isEqualTo(payload.length);
        assertThat(response.getContentAsByteArray()).isEqualTo(payload);
    }

    @Test
    public void countsBytesWrittenToWriter() throws IOException {
        MockHttpServletResponse response = new MockHttpServletResponse();
        response.setCharacterEncoding(StandardCharsets.UTF_8.name());
        CountingServletResponseWrapper wrapper = new CountingServletResponseWrapper(response);

        String payload = "written through the writer";
        PrintWriter writer = wrapper.getWriter();
        writer.write(payload);

        // getBytesWritten() flushes the writer before reading the count.
        assertThat(wrapper.getBytesWritten()).isEqualTo(payload.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    public void countsZeroWhenNothingWritten() {
        MockHttpServletResponse response = new MockHttpServletResponse();
        CountingServletResponseWrapper wrapper = new CountingServletResponseWrapper(response);

        assertThat(wrapper.getBytesWritten()).isEqualTo(0L);
    }
}
