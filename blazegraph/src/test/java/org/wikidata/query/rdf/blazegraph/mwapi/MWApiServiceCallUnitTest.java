package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeConstant;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeVariable;
import static org.wikidata.query.rdf.blazegraph.Matchers.binds;
import static org.wikidata.query.rdf.blazegraph.Matchers.bindsItem;
import static org.wikidata.query.rdf.blazegraph.Matchers.notBinds;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.client.HttpClient;
import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.blazegraph.AbstractBlazegraphTestBase;
import org.wikidata.query.rdf.blazegraph.mwapi.ApiTemplate.OutputVariable;
import org.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceCall.ResultWithContinue;
import org.xml.sax.SAXException;

import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.rdf.internal.impl.literal.XSDNumericIV;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

public class MWApiServiceCallUnitTest extends AbstractBlazegraphTestBase {
    private ApiTemplate template;
    private IBindingSet binding;
    private BigdataValueFactory vf;
    private ServiceConfig config;

    @Before
    public void createFixtures() {
        template = mock(ApiTemplate.class);
        binding = new HashBindingSet();
        vf = store().getValueFactory();
        config = mock(ServiceConfig.class);
    }

    @Test
    public void testFixedParams() throws Exception {
        Map<String, String> fixedMap = ImmutableMap.of("test1", "val1", "test2",
                "val2");
        when(template.getFixedParams()).thenReturn(fixedMap);

        Map<String, String> params = createCall().getRequestParams(binding);
        assertThat(params.entrySet(), equalTo(fixedMap.entrySet()));
    }

    @Test
    public void testInputParams() throws Exception {
        Map<String, IVariableOrConstant> inputVars = new HashMap<>();
        inputVars.put("const", makeConstant(vf, "val1"));
        inputVars.put("var", makeVariable("boundVar"));
        inputVars.put("varDefault", null);
        inputVars.put("emptyDefault", null);

        binding.set(makeVariable("boundVar"), makeConstant(vf, "boundValue"));

        when(template.getInputDefault("varDefault")).thenReturn("defaultValue");
        when(template.getInputDefault("emptyDefault")).thenReturn("");

        Map<String, String> params = createCall(inputVars).getRequestParams(binding);
        assertThat(params, hasEntry("const", "val1"));
        assertThat(params, hasEntry("varDefault", "defaultValue"));
        assertThat(params, hasEntry("var", "boundValue"));
        assertThat(params, not(hasKey("emptyDefault")));
    }

    @Test
    public void testInputParamsUnboundDefault() throws Exception {
        // Variable declared as bound but isn't actually bound - fallback to default
        Map<String, IVariableOrConstant> inputVars = new HashMap<>();
        inputVars.put("var", makeVariable("boundVar"));
        when(template.getInputDefault("var")).thenReturn("defaultValue");
        Map<String, String> params = createCall(inputVars).getRequestParams(binding);
        assertThat(params, hasEntry("var", "defaultValue"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInputParamsUnbound() throws Exception {
        // Variable declared as bound but isn't actually bound
        Map<String, IVariableOrConstant> inputVars = new HashMap<>();
        when(template.isRequiredParameter("var")).thenReturn(true);
        inputVars.put("var", makeVariable("boundVar"));
        Map<String, String> params = createCall(inputVars).getRequestParams(binding);
    }

    @Test
    public void testInputParamsUnboundNotRequired() throws Exception {
        // Variable declared as bound but isn't actually bound
        // If it's not required it's OK
        Map<String, IVariableOrConstant> inputVars = new HashMap<>();
        when(template.isRequiredParameter("var")).thenReturn(false);
        inputVars.put("var", makeVariable("boundVar"));
        Map<String, String> params = createCall(inputVars).getRequestParams(binding);
        assertFalse(params.containsKey("var"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInputParamsMissing() throws Exception {
        // Variable declared but has no binding
        Map<String, IVariableOrConstant> inputVars = new HashMap<>();
        when(template.isRequiredParameter("var")).thenReturn(true);
        inputVars.put("var", null);
        Map<String, String> params = createCall(inputVars).getRequestParams(binding);
    }

    @Test
    public void testEmptyVars() throws Exception {
        List<OutputVariable> outputVars = Lists.newArrayList();
        InputStream responseStream = new ByteArrayInputStream("not even xml".getBytes("UTF-8"));
        Object results = createCall(outputVars).parseResponse(responseStream, binding, 0);
        assertNull(results);
    }

    @Test
    public void testEmptyResult() throws Exception {
        List<OutputVariable> outputVars = ImmutableList
                .of(new OutputVariable(makeVariable("var"), "@test"));
        InputStream responseStream = new ByteArrayInputStream("<result></result>".getBytes("UTF-8"));
        when(template.getItemsPath()).thenReturn("/api/result");
        Iterator<IBindingSet> results = createCall(outputVars).parseResponse(responseStream, binding, 0).getResultIterator();
        assertFalse(results.hasNext());
    }

    @Test
    public void testResults() throws Exception {
        List<OutputVariable> outputVars = ImmutableList.of(
                new OutputVariable(makeVariable("var"), "@name"),
                new OutputVariable(makeVariable("header"), "/api/header/@value"),
                new OutputVariable(OutputVariable.Type.ITEM, makeVariable("item"), "@id"),
                new OutputVariable(OutputVariable.Type.ORDINAL, makeVariable("num"), "@id")
        );
        when(template.getItemsPath()).thenReturn("/api/result");
        InputStream responseStream = new ByteArrayInputStream(
                "<api><header value=\"heading\"></header><result name=\"result1\" id=\"Q1\"></result><result name=\"result2\"></result></api>"
                        .getBytes("UTF-8"));

        Iterator<IBindingSet> results = createCall(outputVars).parseResponse(responseStream, binding, 3).getResultIterator();
        assertTrue(results.hasNext());
        IBindingSet result = results.next();
        assertThat(result, binds("var", "result1"));
        assertThat(result, binds("header", "heading"));
        assertThat(result, bindsItem("item", "Q1"));
        assertThat(result, binds("num", new XSDNumericIV<>(3)));
        result = results.next();
        assertThat(result, binds("var", "result2"));
        assertThat(result, binds("header", "heading"));
        assertThat(result, binds("num", new XSDNumericIV<>(4)));
        assertFalse(results.hasNext());
    }

    @Test(expected = SAXException.class)
    public void testResultsBadXML() throws Exception {
        List<OutputVariable> outputVars = ImmutableList
                .of(new OutputVariable(makeVariable("var"), "@test"));
        InputStream responseStream = new ByteArrayInputStream("Fatal error: I am a teapot".getBytes("UTF-8"));
        when(template.getItemsPath()).thenReturn("/api/result");
        Object results = createCall(outputVars).parseResponse(responseStream, binding, 0);
    }

    @Test
    public void testResultsMissingVar() throws Exception {
        List<OutputVariable> outputVars = ImmutableList.of(
                new OutputVariable(makeVariable("var"), "@name"),
                new OutputVariable(makeVariable("data"), "text()"),
                new OutputVariable(makeVariable("header"), "/api/header/@value"));
        when(template.getItemsPath()).thenReturn("/api/result");
        InputStream responseStream = new ByteArrayInputStream(
                "<api><header value=\"heading\"></header><result name=\"result1\">datadata</result><result>we need moar data</result></api>"
                        .getBytes("UTF-8"));

        Iterator<IBindingSet> results = createCall(outputVars).parseResponse(responseStream, binding, 0).getResultIterator();
        assertTrue(results.hasNext());
        IBindingSet result = results.next();
        assertThat(result, binds("var", "result1"));
        assertThat(result, binds("data", "datadata"));
        assertThat(result, binds("header", "heading"));
        result = results.next();
        assertThat(result, notBinds("var"));
        assertThat(result, binds("data", "we need moar data"));
        assertThat(result, binds("header", "heading"));
        assertFalse(results.hasNext());
    }

    @Test
    public void testResultsNoContinue() throws Exception {
        List<OutputVariable> outputVars = ImmutableList.of(new OutputVariable(makeVariable("var"), "@name"));
        when(template.getItemsPath()).thenReturn("/api/result");
        InputStream responseStream = new ByteArrayInputStream(
                "<api><header value=\"heading\"></header><result name=\"result1\">datadata</result></api>"
                        .getBytes("UTF-8"));
        ResultWithContinue results = createCall(outputVars).parseResponse(responseStream, binding, 0);
        assertNull(results.getContinue());
    }

    @Test
    public void testResultsWithContinue() throws Exception {
        List<OutputVariable> outputVars = ImmutableList.of(new OutputVariable(makeVariable("var"), "@name"));
        when(template.getItemsPath()).thenReturn("/api/result");
        InputStream responseStream = new ByteArrayInputStream(
                "<api><continue sroffset=\"5\" continue=\"-||\"></continue><header value=\"heading\"></header><result name=\"result1\">datadata</result></api>"
                        .getBytes("UTF-8"));
        ResultWithContinue results = createCall(outputVars).parseResponse(responseStream, binding, 0);
        Map<String, String> continueMap = results.getContinue();
        assertThat(continueMap.keySet(), containsInAnyOrder("sroffset", "continue"));
        assertThat(continueMap, hasEntry("sroffset", "5"));
        assertThat(continueMap, hasEntry("continue", "-||"));
    }

    private MWApiServiceCall createCall() throws Exception {
        return createCall(Maps.newHashMap(), Lists.newArrayList());
    }

    private MWApiServiceCall createCall(Map<String, IVariableOrConstant> inputVars) throws Exception {
        return createCall(inputVars, Lists.newArrayList());
    }

    private MWApiServiceCall createCall(List<OutputVariable> outputVars) throws Exception {
        return createCall(Maps.newHashMap(), outputVars);
    }

    private Endpoint makeEndpoint(String name) throws MalformedURLException {
        when(config.validEndpoint(name)).thenReturn(true);
        return Endpoint.create(makeConstant(vf, name), config);
    }

    private MWApiServiceCall createCall(
            Map<String, IVariableOrConstant> inputVars,
            List<OutputVariable> outputVars) throws Exception {
        return createCall(inputVars, outputVars, new MWApiLimits(10000, 1000, 1000));
    }

    private MWApiServiceCall createCall(
            Map<String, IVariableOrConstant> inputVars,
            List<OutputVariable> outputVars, MWApiLimits limits) throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        Timer requestTimer = mock(Timer.class);
        return new MWApiServiceCall(template, makeEndpoint("acme.test"), inputVars,
                outputVars, mockClient, store().getLexiconRelation(), requestTimer, limits);
    }
}
