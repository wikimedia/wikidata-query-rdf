package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.hamcrest.Matchers.equalTo;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeConstant;
import static org.junit.Assert.assertThat;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeVariable;

import java.net.MalformedURLException;

import org.junit.Before;
import org.junit.Test;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.blazegraph.AbstractBlazegraphTestBase;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.rdf.model.BigdataValueFactory;

public class EndpointUnitTest extends AbstractBlazegraphTestBase {

    private ServiceConfig config;
    private BigdataValueFactory vf;
    private IBindingSet binding;

    @Before
    public void createFixtures() {
        config = mock(ServiceConfig.class);
        vf = store().getValueFactory();
        binding = new HashBindingSet();
    }

    @Test
    public void testConstant() throws MalformedURLException {
        when(config.validEndpoint("endpoint.test")).thenReturn(true);
        Endpoint ep = Endpoint.create(makeConstant(vf, "endpoint.test"), config);
        String url = ep.getEndpointURL(binding);
        assertThat(url, equalTo("https://endpoint.test/w/api.php"));
    }

    @Test
    public void testConstantFromURI() throws MalformedURLException {
        when(config.validEndpoint("endpoint.test")).thenReturn(true);
        Endpoint ep = Endpoint.create(makeConstant(vf, new URIImpl("http://endpoint.test/blah")), config);
        String url = ep.getEndpointURL(binding);
        assertThat(url, equalTo("https://endpoint.test/w/api.php"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstantNotAllowed() throws MalformedURLException {
        when(config.validEndpoint("endpoint.test")).thenReturn(false);
        Endpoint ep = Endpoint.create(makeConstant(vf, "endpoint.test"), config);
        String url = ep.getEndpointURL(binding);
    }

    // Not testing bad URL because apparently java.net.URL will accept any hostname

    @Test
    public void testVar() throws MalformedURLException {
        binding.set(makeVariable("endpoint"), makeConstant(vf, "endpoint.test"));
        testVarEndpoint("https://endpoint.test/w/api.php");
    }

    @Test
    public void testVarURI() throws MalformedURLException {
        binding.set(makeVariable("endpoint"), makeConstant(vf, new URIImpl("http://endpoint.test/blah")));
        testVarEndpoint("https://endpoint.test/w/api.php");
    }

    @Test
    public void testVarNotBound() throws MalformedURLException {
        testVarEndpoint(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testVarNotAllowed() throws MalformedURLException {
        binding.set(makeVariable("endpoint"), makeConstant(vf, new URIImpl("http://endpoint.not.test/blah")));
        testVarEndpoint(null);
    }

    private void testVarEndpoint(String expected) throws MalformedURLException {
        when(config.validEndpoint("endpoint.test")).thenReturn(true);
        Endpoint ep = Endpoint.create(makeVariable("endpoint"), config);
        String url = ep.getEndpointURL(binding);
        assertThat(url, equalTo(expected));
    }

}
