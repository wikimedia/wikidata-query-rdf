package org.wikidata.query.rdf.blazegraph.categories;

import org.openrdf.model.URI;
import org.openrdf.model.impl.URIImpl;
import org.wikidata.query.rdf.common.uri.Mediawiki;

import com.bigdata.rdf.sparql.ast.eval.ServiceParams;
import com.bigdata.rdf.sparql.ast.service.ServiceCallCreateParams;
import com.bigdata.rdf.sparql.ast.service.ServiceRegistry;
import com.bigdata.rdf.sparql.ast.service.storedquery.SimpleStoredQueryService;

/**
 * Stored query for categories:
 * SELECT ?out ?depth WHERE {
 *  SERVICE mediawiki:categoryTree {
 *      bd:serviceParam mediawiki:start <https://en.wikipedia.org/wiki/Category:Ducks> .
 *      bd:serviceParam mediawiki:direction "Reverse" .
 *      bd:serviceParam mediawiki:depth 5 .
 *  }
 * } ORDER BY ASC(?depth)
 *
 * Directions are:
 * - Forward: get parent category tree
 * - Reverse: get subcategory tree
 * - Undirected: both directions
 */
public class CategoriesStoredQuery extends SimpleStoredQueryService {
    /**
     * The URI service key.
     */
    public static final URI SERVICE_KEY = new URIImpl(Mediawiki.NAMESPACE + "categoryTree");
    /**
     * start parameter.
     */
    public static final URI START_PARAM = new URIImpl(Mediawiki.NAMESPACE + "start");
    /**
     * direction parameter.
     */
    public static final URI DIRECTION_PARAM = new URIImpl(Mediawiki.NAMESPACE + "direction");
    /**
     * max depth parameter.
     */
    public static final URI DEPTH_PARAM = new URIImpl(Mediawiki.NAMESPACE + "depth");
    /**
     * Default max depth.
     */
    public static final int MAX_DEPTH = 8;
    /**
     * Register the service so it is recognized by Blazegraph.
     */
    public static void register() {
        ServiceRegistry reg = ServiceRegistry.getInstance();
        reg.add(SERVICE_KEY, new CategoriesStoredQuery());
        reg.addWhitelistURL(SERVICE_KEY.toString());
    }

    @Override
    protected String getQuery(ServiceCallCreateParams createParams,
            ServiceParams serviceParams) {
        final URI start = serviceParams.getAsURI(START_PARAM);
        final String direction = serviceParams.getAsString(DIRECTION_PARAM, "Reverse");
        final int depth = serviceParams.getAsInt(DEPTH_PARAM, MAX_DEPTH);

        // Fixed parts
        return "SELECT * WHERE {\n" +
                "SERVICE gas:service {\n" +
                "     gas:program gas:gasClass \"com.bigdata.rdf.graph.analytics.BFS\" .\n" +
                "     gas:program gas:linkType mediawiki:isInCategory .\n" +
        // Variable parts
                "   gas:program gas:traversalDirection \"" + direction + "\" .\n" +
                "     gas:program gas:in <" + start.stringValue() + "> .\n" +
                "     gas:program gas:out ?out .\n" +
                "     gas:program gas:out1 ?depth .\n" +
                "     gas:program gas:out2 ?predecessor .\n" +
                "     gas:program gas:maxIterations " + depth + " .\n" +
        // Fixed footer
                "  }\n" +
                "}";
    }

}
