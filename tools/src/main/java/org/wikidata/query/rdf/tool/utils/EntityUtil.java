package org.wikidata.query.rdf.tool.utils;

public final class EntityUtil {

    public static String cleanEntityId(String entityIdWithPrefix) {
        // FIXME: this should not be hardcoded
        if (entityIdWithPrefix.startsWith("Property:")) {
           return entityIdWithPrefix.substring("Property:".length());
        } else if (entityIdWithPrefix.startsWith("Item:")) {
            return entityIdWithPrefix.substring("Item:".length());
        } else if (entityIdWithPrefix.startsWith("Lexeme:")) {
            return entityIdWithPrefix.substring("Lexeme:".length());
        }
        return entityIdWithPrefix;
    }

    private EntityUtil() {}
}
