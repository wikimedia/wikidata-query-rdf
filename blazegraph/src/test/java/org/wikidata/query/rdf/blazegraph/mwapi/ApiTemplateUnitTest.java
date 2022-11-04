package org.wikidata.query.rdf.blazegraph.mwapi;

import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceFactory.paramNameToURI;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.openrdf.model.URI;
import org.wikidata.query.rdf.blazegraph.AbstractBlazegraphTestBase;
import org.wikidata.query.rdf.blazegraph.mwapi.ApiTemplate.OutputVariable;

import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.sparql.ast.JoinGroupNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.VarNode;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class ApiTemplateUnitTest extends AbstractBlazegraphTestBase {

    private static final String JSON_CONFIG = "{\n" +
            "\"params\": {\n"
          + "    \"action\": \"query\",\n"
          + "    \"prop\": \"categories\",\n"
          + "    \"titles\": {\n"
          + "       \"type\": \"list\"\n"
          + "    },\n"
          + "    \"name\": {\n"
          + "       \"type\": \"string\"\n"
          + "    },\n"
          + "    \"cllimit\": {\n"
          + "        \"type\": \"int\",\n"
          + "        \"default\": 500\n"
          + "    },\n"
          + "    \"cldir\": {\n"
          + "        \"type\": \"string\",\n"
          + "        \"default\": \"\"\n"
          + "    }\n"
          + "},\n"
          + "\"output\": {\n"
          + "    \"items\": \"/api/query/pages/page/categories/cl\",\n"
          + "    \"vars\": {\n"
          + "      \"category\": \"@title\",\n"
          + "      \"title\": \"/api/query/pages/page/@title\"\n"
          + "    }\n"
          + "}\n"
          + "}\n";

    /**
     * Make JSON node from string.
     * @param jsonString
     * @return
     * @throws JsonProcessingException
     * @throws IOException
     */
    private JsonNode parseJson(String jsonString) throws JsonProcessingException, IOException {
        ObjectMapper mapper = new ObjectMapper();
        return mapper.readTree(jsonString);
    }

    @Test
    public void testServiceInput() throws Exception {
        JsonNode json = parseJson(JSON_CONFIG);

        ApiTemplate template = ApiTemplate.fromJSON(json);
        Map<String, String> fixed = template.getFixedParams();
        // Fixed params
        assertThat(fixed, hasKey("action"));
        assertThat(fixed, hasKey("prop"));
        assertThat(fixed, not(hasKey("cllimit")));
        assertThat(fixed, not(hasKey("titles")));
        // Input params with default
        assertThat(template.getInputDefault("cllimit"), equalTo("500"));
        assertThat(template.getInputDefault("cldir"), equalTo(""));
        assertNull(template.getInputDefault("titles"));
        // Bound params
        ServiceParams serviceParams = new ServiceParams();
        serviceParams.add(paramNameToURI("titles"), createConstant("sometitle"));
        serviceParams.add(paramNameToURI("name"), new VarNode("somevar"));
        serviceParams.add(paramNameToURI("ducks"), createConstant("extraParam"));
        Map<String, IVariableOrConstant> input = template.getInputVars(serviceParams);
        assertThat(input, hasKey("titles"));
        assertThat(input, hasKey("name"));
        assertThat(input, hasKey("cllimit"));
        assertThat(input, hasKey("ducks"));
        assertThat(input, hasKey("cldir"));
        // Bound constant
        assertTrue(input.get("titles").isConstant());
        assertTrue(input.get("ducks").isConstant());
        // Bound var
        assertTrue(input.get("name").isVar());
        assertThat(input.get("name").getName(), equalTo("somevar"));
        // Unbound vars have nulls
        assertNull(input.get("cllimit"));
        assertNull(input.get("cldir"));
    }

    @Test
    public void testServiceOutput() throws Exception {
        JsonNode json = parseJson(JSON_CONFIG);

        ApiTemplate template = ApiTemplate.fromJSON(json);
        assertThat(template.getItemsPath(), equalTo("/api/query/pages/page/categories/cl"));

        JoinGroupNode patterns = new JoinGroupNode();
        // predefined variable
        patterns.addArg(new StatementPatternNode(
                new VarNode("somevar"),
                createURI(ApiTemplate.OutputVariable.Type.STRING.predicate()),
                createURI(paramNameToURI("category"))
        ));
        // User-defined variable
        patterns.addArg(new StatementPatternNode(
                new VarNode("var2"),
                createURI(ApiTemplate.OutputVariable.Type.URI.predicate()),
                createConstant("@somedata")
        ));
        // User-defined path variable
        patterns.addArg(new StatementPatternNode(
                new VarNode("var3"),
                createURI(ApiTemplate.OutputVariable.Type.ITEM.predicate()),
                createConstant("item/@wikibase_id")
        ));
        // Variable with ordinal
        patterns.addArg(new StatementPatternNode(
                new VarNode("var4"),
                createURI(ApiTemplate.OutputVariable.Type.ORDINAL.predicate()),
                createConstant("goat")
        ));

        ServiceNode serviceNode = new ServiceNode(createConstant("test"), patterns);

        List<OutputVariable> outputs = template.getOutputVars(serviceNode);
        assertThat(outputs.size(), equalTo(4));
        // Pre-defined variable
        OutputVariable outputVar = outputs.get(0);
        assertThat(outputVar.getName(), equalTo("somevar"));
        assertThat(outputVar.getPath(), equalTo("@title"));
        assertFalse(outputVar.isOrdinal());
        // User-defined variable
        outputVar = outputs.get(1);
        assertThat(outputVar.getName(), equalTo("var2"));
        assertThat(outputVar.getPath(), equalTo("@somedata"));
        assertTrue(outputVar.isURI());
        assertFalse(outputVar.isOrdinal());
        assertThat(outputVar.getURI("http://test.com/"), instanceOf(URI.class));
        // URI keeps the case
        assertThat(outputVar.getURI("http://test.com/test").toString(), endsWith("test"));
        // User-defined variable which is an item
        outputVar = outputs.get(2);
        assertThat(outputVar.getName(), equalTo("var3"));
        assertThat(outputVar.getPath(), equalTo("item/@wikibase_id"));
        assertTrue(outputVar.isURI());
        assertFalse(outputVar.isOrdinal());
        assertThat(outputVar.getURI("test"), instanceOf(URI.class));
        // T172642: Item URIs will be uppercased
        assertThat(outputVar.getURI("test").toString(), endsWith("TEST"));
        // Ordinal
        outputVar = outputs.get(3);
        assertThat(outputVar.getName(), equalTo("var4"));
        assertThat(outputVar.getPath(), equalTo("."));
        assertFalse(outputVar.isURI());
        assertTrue(outputVar.isOrdinal());

    }

    @Test(expected = NullPointerException.class)
    public void testNoParams() throws Exception {
        JsonNode json = parseJson("{}");
        ApiTemplate.fromJSON(json);
    }

    @Test(expected = NullPointerException.class)
    public void testNoOutput() throws Exception {
        JsonNode json = parseJson("{\"params\": {}}");
        ApiTemplate.fromJSON(json);
    }

    @Test(expected = NullPointerException.class)
    public void testNoItems() throws Exception {
        JsonNode json = parseJson("{\"params\": {}, \"output\": {}}");
        ApiTemplate.fromJSON(json);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testDuplicateItems() throws Exception {
        JsonNode json = parseJson(
                "{\"params\": {\"test\": \"\"}, \"output\": {\"items\":\"\", \"vars\": {\"test\":\"\"}}}");
        ApiTemplate.fromJSON(json);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testMissingInputVar() throws Exception {
        JsonNode json = parseJson(JSON_CONFIG);

        ApiTemplate template = ApiTemplate.fromJSON(json);
        ServiceParams serviceParams = new ServiceParams();
        serviceParams.add(paramNameToURI("titles"), createConstant("sometitle"));
        template.getInputVars(serviceParams);
    }
}
