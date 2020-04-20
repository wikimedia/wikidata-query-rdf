package org.wikidata.query.rdf.blazegraph.mwapi;

import static java.util.Objects.requireNonNull;
import static org.wikidata.query.rdf.blazegraph.mwapi.ApiTemplate.OutputVariable.Type.ORDINAL;
import static org.wikidata.query.rdf.blazegraph.mwapi.MWApiServiceFactory.paramNameToURI;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Ontology;
import org.wikidata.query.rdf.common.uri.UrisSchemeFactory;

import com.bigdata.bop.IVariable;
import com.bigdata.bop.IVariableOrConstant;
import com.bigdata.rdf.internal.IV;
import com.bigdata.rdf.sparql.ast.GraphPatternGroup;
import com.bigdata.rdf.sparql.ast.IGroupMemberNode;
import com.bigdata.rdf.sparql.ast.StatementPatternNode;
import com.bigdata.rdf.sparql.ast.TermNode;
import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceNode;
import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * This class represents API template.
 */
@SuppressFBWarnings(value = "FCCD_FIND_CLASS_CIRCULAR_DEPENDENCY", justification = "low priority to fix")
public class ApiTemplate {
    /**
     * Set of fixed API parameters.
     */
    private final Map<String, String> fixedParams;
    /**
     * Set of API parameters that should come from input vars.
     */
    private final Set<String> inputVars;
    /**
     * Set of defaults for API parameters that were not bound.
     */
    private final Map<String, String> defaults;
    /**
     * Set of API parameters that should be sent to output.
     * The value is the XPath to find the value.
     */
    private final Map<String, String> outputVars;
    /**
     * XPath to result items.
     */
    private final String items;

    /**
     * Hidden ctor.
     * Use fromJSON() to create the object.
     */
    protected ApiTemplate(Map<String, String> fixedParams,
            Set<String> inputVars, Map<String, String> defaults,
            Map<String, String> outputVars, String items) {
        this.fixedParams = fixedParams;
        this.inputVars = inputVars;
        this.defaults = defaults;
        this.outputVars = outputVars;
        this.items = items;

    }

    /**
     * Create API template from JSON configuration.
     */
    public static ApiTemplate fromJSON(JsonNode json) {
        Map<String, String> fixedParams = new HashMap<>();
        Set<String> inputVars = new HashSet<>();
        Map<String, String> defaults = new HashMap<>();
        Map<String, String> outputVars = new HashMap<>();
        // Parse input params
        final JsonNode params = json.get("params");
        requireNonNull(params, "Missing params node");
        params.fieldNames().forEachRemaining(paramName -> {
            if (fixedParams.containsKey(paramName)
                    || inputVars.contains(paramName)) {
                throw new IllegalArgumentException(
                        "Repeated input parameter " + paramName);
            }

            JsonNode value = params.get(paramName);
            // scalar value means fixed parameter
            if (value.isValueNode()) {
                fixedParams.put(paramName, value.asText());
                return;
            }
            // otherwise it's a parameter
            // FIXME: ignoring type for now
            inputVars.add(paramName);
            if (value.has("default")) {
                defaults.put(paramName, value.get("default").asText());
            }
        });

        // Parse output params
        final JsonNode output = json.get("output");
        requireNonNull(params, "Missing output node");
        String items = output.get("items").asText();
        final JsonNode vars = output.get("vars");
        requireNonNull(vars, "Missing vars node");
        vars.fieldNames().forEachRemaining(paramName -> {
            if (inputVars.contains(paramName)
                    || fixedParams.containsKey(paramName)) {
                throw new IllegalArgumentException("Parameter " + paramName
                        + " declared as both input and output");
            }
            outputVars.put(paramName, vars.get(paramName).asText());

        });

        return new ApiTemplate(ImmutableMap.copyOf(fixedParams),
                ImmutableSet.copyOf(inputVars), ImmutableMap.copyOf(defaults),
                ImmutableMap.copyOf(outputVars), items);
    }

    /**
     * Get items XPath.
     */
    public String getItemsPath() {
        return items;
    }

    /**
     * Check if parameter is required.
     */
    public boolean isRequiredParameter(String name) {
        return inputVars.contains(name);
    }

    /**
     * Get call fixed parameters.
     */
    public Map<String, String> getFixedParams() {
        return fixedParams;
    }

    /**
     * Find default for this parameter.
     *
     * @return Default value or null.
     */
    public String getInputDefault(String name) {
        return defaults.get(name);
    }

    /**
     * Add input var from the service params to the map.
     * @param vars Target Map
     * @param varName Parameter name
     * @param iVar Parameter node (can be null if it's pre-defined but not specified)
     */
    private void addInputVar(Map<String, IVariableOrConstant> vars, String varName, TermNode iVar) {
        if (iVar == null) {
            if (!defaults.containsKey(varName)) {
                // Param should have either binding or default
                throw new IllegalArgumentException("Parameter " + varName + " must be bound");
            }
            // If var is null but we have a default, put null there, service call will know
            // how to handle it.
            vars.put(varName, null);
        } else {
            if (!iVar.isConstant() && !iVar.isVariable()) {
                // Binding should be constant or var
                throw new IllegalArgumentException("Parameter " + varName + " must be constant or variable");
            }
            vars.put(varName, iVar.getValueExpression());
        }
    }

    /**
     * Create list of bindings from input params to specific variables or constants.
     * @param serviceParams Specific invocation params.
     * @return Map of bindings, which has constant or variable from service params if bound, or null if not bound.
     */
    public Map<String, IVariableOrConstant> getInputVars(final ServiceParams serviceParams) {
        Map<String, IVariableOrConstant> vars = Maps.newHashMapWithExpectedSize(inputVars.size());

        String prefix = paramNameToURI("").stringValue();
        // Collect pre-defined vars
        for (String entry : inputVars) {
            addInputVar(vars, entry, serviceParams.get(paramNameToURI(entry), null));
        }
        // Now collect new vars
        // TODO: think about how to better unite these two loops
        serviceParams.iterator().forEachRemaining(param -> {
            String paramNameFull = param.getKey().stringValue();
            if (!paramNameFull.startsWith(prefix)) {
                return;
            }
            String paramName = paramNameFull.substring(prefix.length());
            if (param.getValue().size() > 1) {
                throw new IllegalArgumentException("Parameter " + paramName + " is duplicated");
            }
            if (vars.containsKey(paramName)) {
                // already taken care of
                return;
            }
            addInputVar(vars, paramName, param.getValue().get(0));
        });

        return vars;
    }

    /**
     * Create map of output variables from template and service params.
     */
    public List<OutputVariable> getOutputVars(final ServiceNode serviceNode) {
        List<OutputVariable> vars = new ArrayList<>(outputVars.size());

        final GraphPatternGroup<IGroupMemberNode> group = serviceNode.getGraphPattern();
        requireNonNull(serviceNode, "Group node is null?");

        String prefix = paramNameToURI("").stringValue();
        group.iterator().forEachRemaining(node -> {
            // Ouptut nodes are:
            // ?variable wikibase:output mwapi:title
            // or:
            // ?variable wikibase:output "x/path"
            if (node instanceof StatementPatternNode) {
                final StatementPatternNode sp = (StatementPatternNode) node;

                if (sp.s().isVariable() && sp.o().isConstant() && sp.p().isConstant()) {
                    for (OutputVariable.Type varType : OutputVariable.Type.values()) {
                        if (varType.predicate.equals(sp.p().getValue())) {
                            IVariable v = (IVariable)sp.s().getValueExpression();
                            if (varType == ORDINAL) {
                                // Ordinal values ignore the object
                                vars.add(new OutputVariable(varType, v, "."));
                                break;
                             }
                            IV value = sp.o().getValueExpression().get();
                            if (value.isURI()) {
                                String paramName = value.stringValue().substring(prefix.length());
                                vars.add(new OutputVariable(varType, v, outputVars.get(paramName)));
                            } else {
                                vars.add(new OutputVariable(varType, v, value.stringValue()));
                            }
                            break;
                        }
                    }
                }
            }
        });

        return vars;
    }

    /**
     * Variable in the output of the API.
     */
    public static class OutputVariable {

        /**
         * Type of variable result.
         */
        public enum Type {
            /**
             * Plain string var.
             */
            STRING("apiOutput"),
            /**
             * Var transformed to URI.
             */
            URI("apiOutputURI"),
            /**
             * Item ID.
             */
            ITEM("apiOutputItem"),
            /**
             * Ordinal - i.e. place of the result in the list.
             */
            ORDINAL("apiOrdinal");

            /**
             * Predicate used for this type.
             */
            private final URI predicate;
            Type(String predicate) {
                this.predicate = new URIImpl(Ontology.NAMESPACE + predicate);
            }

            /**
             * Get predicate.
             * @return Predicate URI
             */
            URI predicate() {
                return predicate;
            }

            @Override
            public String toString() {
                return predicate.stringValue();
            }

        }
        /**
         * Original Blazegraph var.
         */
        private final IVariable iVar;
        /**
         * Path expression to extract value from result.
         * The path is relative to items in template.
         * Currently XPath syntax is being used.
         */
        private final String path;

        /**
         * Variable type.
         * Can be just string, URI or item ID.
         */
        private final Type type;

        public OutputVariable(Type type, IVariable iVar, String xpath) {
            this.iVar = iVar;
            this.path = xpath;
            this.type = type;
        }

        public OutputVariable(IVariable iVar, String xpath) {
            this(Type.STRING, iVar, xpath);
        }

        /**
         * Get associated variable.
         */
        public IVariable getVar() {
            return iVar;
        }

        /**
         * Get path to this variable.
         */
        public String getPath() {
            return path;
        }

        /**
         * Get associated variable name.
         */
        public String getName() {
            return iVar.getName();
        }

        @Override
        public String toString() {
            return getName() + "(" + getPath() + ")";
        }

        /**
         * Is it the ordinal value?
         */
        public boolean isOrdinal() {
            return type == ORDINAL;
        }

        /**
         * Would this variable produce an URI?
         */
        public boolean isURI() {
            return type != Type.STRING && type != ORDINAL;
        }

        /**
         * Get URI value matching variable type.
         */
        public URI getURI(String value) {
            switch (type) {
            case URI:
                return new URIImpl(value);
            case ITEM:
                return new URIImpl(UrisSchemeFactory.getURISystem().entityIdToURI(value.toUpperCase(Locale.ROOT)));
            default:
                throw new IllegalArgumentException("Can not produce URI for non-URI type " + type);
            }
        }
    }

}
