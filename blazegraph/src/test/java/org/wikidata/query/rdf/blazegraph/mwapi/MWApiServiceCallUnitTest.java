package org.wikidata.query.rdf.blazegraph.mwapi;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.jetty.client.HttpClient;
import org.junit.Before;
import org.junit.Test;
import org.wikidata.query.rdf.blazegraph.AbstractRandomizedBlazegraphTestBase;
import org.wikidata.query.rdf.blazegraph.mwapi.ApiTemplate.OutputVariable;
import org.xml.sax.SAXException;

import com.beust.jcommander.internal.Lists;
import com.beust.jcommander.internal.Maps;
import com.bigdata.bop.IBindingSet;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.bop.bindingSet.HashBindingSet;
import com.bigdata.rdf.model.BigdataValueFactory;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeConstant;
import static org.wikidata.query.rdf.blazegraph.BigdataValuesHelper.makeVariable;
import static org.wikidata.query.rdf.blazegraph.Matchers.binds;
import static org.wikidata.query.rdf.blazegraph.Matchers.bindsItem;
import static org.wikidata.query.rdf.blazegraph.Matchers.notBinds;

public class MWApiServiceCallUnitTest extends AbstractRandomizedBlazegraphTestBase {
    private ApiTemplate template;
    private IBindingSet binding;
    private BigdataValueFactory vf;

    @Before
    public void createFixtures() {
        template = mock(ApiTemplate.class);
        binding = new HashBindingSet();
        vf = store().getValueFactory();
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
        IBindingSet[] results = createCall(outputVars).parseResponse(responseStream, binding);
        assertNull(results);
    }

    @Test
    public void testEmptyResult() throws Exception {
        List<OutputVariable> outputVars = ImmutableList
                .of(new OutputVariable(makeVariable("var"), "@test"));
        InputStream responseStream = new ByteArrayInputStream("<result></result>".getBytes("UTF-8"));
        when(template.getItemsPath()).thenReturn("/api/result");
        IBindingSet[] results = createCall(outputVars).parseResponse(responseStream, binding);
        assertNull(results);
    }

    @Test
    public void testResults() throws Exception {
        List<OutputVariable> outputVars = ImmutableList.of(
                new OutputVariable(makeVariable("var"), "@name"),
                new OutputVariable(makeVariable("header"), "/api/header/@value"),
                new OutputVariable(OutputVariable.Type.ITEM, makeVariable("item"), "@id")
                );
        when(template.getItemsPath()).thenReturn("/api/result");
        InputStream responseStream = new ByteArrayInputStream(
                "<api><header value=\"heading\"></header><result name=\"result1\" id=\"Q1\"></result><result name=\"result2\"></result></api>"
                        .getBytes("UTF-8"));

        IBindingSet[] results = createCall(outputVars).parseResponse(responseStream, binding);
        assertThat(results.length, equalTo(2));
        assertThat(results[0], binds("var", "result1"));
        assertThat(results[0], binds("header", "heading"));
        assertThat(results[0], bindsItem("item", "Q1"));
        assertThat(results[1], binds("var", "result2"));
        assertThat(results[1], binds("header", "heading"));
    }

    @Test(expected = SAXException.class)
    public void testResultsBadXML() throws Exception {
        List<OutputVariable> outputVars = ImmutableList
                .of(new OutputVariable(makeVariable("var"), "@test"));
        InputStream responseStream = new ByteArrayInputStream("Fatal error: I am a teapot".getBytes("UTF-8"));
        when(template.getItemsPath()).thenReturn("/api/result");
        IBindingSet[] results = createCall(outputVars).parseResponse(responseStream, binding);
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

        IBindingSet[] results = createCall(outputVars).parseResponse(responseStream, binding);
        assertThat(results.length, equalTo(2));
        assertThat(results[0], binds("var", "result1"));
        assertThat(results[0], binds("data", "datadata"));
        assertThat(results[0], binds("header", "heading"));
        assertThat(results[1], notBinds("var"));
        assertThat(results[1], binds("data", "we need moar data"));
        assertThat(results[1], binds("header", "heading"));
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

    private MWApiServiceCall createCall(
            Map<String, IVariableOrConstant> inputVars,
            List<OutputVariable> outputVars) throws Exception {
        HttpClient mockClient = mock(HttpClient.class);
        return new MWApiServiceCall(template, "acme.test", inputVars,
                outputVars, mockClient, store().getLexiconRelation());
    }

}
