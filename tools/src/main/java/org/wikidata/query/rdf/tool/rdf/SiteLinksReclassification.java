package org.wikidata.query.rdf.tool.rdf;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static java.util.stream.Collectors.toSet;
import static org.wikidata.query.rdf.tool.rdf.StatementPredicates.isAboutResource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.openrdf.model.Resource;
import org.openrdf.model.Statement;

import com.google.common.collect.Sets;

/**
 * Hack to circumvent the "shared" nature of sitelinks.
 * In some cases sitelinks might be moved from one entity to another
 * during a "merge" operation.
 * This merge is not yet identified in the producer.
 */
public final class SiteLinksReclassification {
    private SiteLinksReclassification() {}

    /**
     * Hack to reclassify statements related to sitelinks.
     *
     * Move all sitelinks statements to linkedShared
     * Move sitelink statements to delete not identified as renames to unlinkedSharedStatements
     */
    public static Patch reclassify(Patch inputPatch) {
        Map<Resource, SiteLinkBlock> addedSitelinks = extractSiteLinks(inputPatch.getAdded());
        Map<Resource, SiteLinkBlock> deletedSitelinks = extractSiteLinks(inputPatch.getRemoved());
        Set<Resource> renames = Sets.intersection(addedSitelinks.keySet(), deletedSitelinks.keySet());

        Set<Statement> deletedAndNotRenamed = deletedSitelinks.entrySet().stream()
                .filter(e -> !renames.contains(e.getKey()))
                .map(Map.Entry::getValue)
                .flatMap(slb -> slb.getStatements().stream())
                .collect(toSet());

        Set<Statement> addToLinkedShared = addedSitelinks.values().stream()
                .map(SiteLinkBlock::getStatements)
                .flatMap(Collection::stream)
                .collect(toSet());

        List<Statement> added = new ArrayList<>(Sets.difference(new HashSet<>(inputPatch.getAdded()), addToLinkedShared));
        List<Statement> linkedShared = new ArrayList<>(inputPatch.getLinkedSharedElements());
        linkedShared.addAll(addToLinkedShared);

        List<Statement> deleted = new ArrayList<>(Sets.difference(new HashSet<>(inputPatch.getRemoved()), deletedAndNotRenamed));
        List<Statement> unlinkedShared = new ArrayList<>(inputPatch.getUnlinkedSharedElements());
        unlinkedShared.addAll(deletedAndNotRenamed);

        return new Patch(added, linkedShared, deleted, unlinkedShared);
    }

    /**
     * Sitelink blocks are as follow
     * <pre>
     * &lt;https://ce.wikipedia.org/wiki/Foo&gt; a schema:Article ;
	 *   schema:about wd:Q25505610 ;
	 *   schema:inLanguage "ce" ;
	 *   schema:isPartOf <https://ce.wikipedia.org/> ;
     *   schema:name "Верин Хотаван"@ce .
     * </pre>
     * Excluding the schema:about triple as it's related to the entity
     */
    private static Map<Resource, SiteLinkBlock> extractSiteLinks(List<Statement> statements) {
        return statements.stream()
                .filter(StatementPredicates::articleType)
                .map(Statement::getSubject)
                .map(siteLink -> {
                    List<Statement> allButSiteLinkToEntity = statements.stream()
                            .filter(s -> s.getSubject().equals(siteLink))
                            .filter(s -> !isAboutResource(s))
                            .collect(toList());
                    Resource site = allButSiteLinkToEntity.stream()
                            .filter(StatementPredicates::isPartOf)
                            .map(Statement::getObject)
                            .filter(Resource.class::isInstance)
                            .map(Resource.class::cast)
                            .findFirst()
                            .orElseThrow(() -> new IllegalArgumentException("Cannot find schema:isPartOf for sitelink " + siteLink));
                    return new SiteLinkBlock(siteLink, site, allButSiteLinkToEntity);
                })
                .collect(toMap(SiteLinkBlock::getSite, identity(), (e1, e2) -> {
                    throw new IllegalArgumentException("Same site [" + e1.getSite() + "] for " +
                            "sitelink " + e1.getSiteLink().stringValue() +
                            " and " + e2.getSiteLink().stringValue());
                }));
    }

    private static class SiteLinkBlock {
        private final Resource siteLink;
        private final Resource site;
        private final List<Statement> statements;

        SiteLinkBlock(Resource siteLink, Resource site, List<Statement> statements) {
            this.siteLink = siteLink;
            this.site = site;
            this.statements = statements;
        }

        public Resource getSiteLink() {
            return siteLink;
        }

        public Resource getSite() {
            return site;
        }

        public List<Statement> getStatements() {
            return statements;
        }
    }
}
