package org.wikidata.query.rdf.tool;

import java.io.IOException;
import java.util.List;
import java.util.regex.Pattern;

import org.wikidata.query.rdf.tool.exception.InvalidEntityIdException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.collect.Lists;

import lombok.Data;

@Data
@JsonSerialize(using = EntityId.Serializer.class)
@JsonDeserialize(using = EntityId.Deserializer.class)
public class EntityId {
    // FIXME: this list should not be hardcoded
    private static final List<String> WIKIDATA_ENTITY_NAMESPACE_NAMES = Lists.newArrayList("Property", "Item", "Lexeme");
    private static final Pattern IS_VALID = Pattern.compile("[A-Z]\\d+");
    private final String prefix;
    private final long id;

    public static EntityId parse(String id) {
        try {
            long parsed = Long.parseLong(id.substring(1), 10);
            return new EntityId(id.substring(0, 1), parsed);
        } catch (NumberFormatException e) {
            throw new InvalidEntityIdException(id, e);
        }
    }

    public static boolean isValidId(String maybeId) {
        return IS_VALID.matcher(maybeId).matches();
    }

    public static String cleanEntityId(String entityIdWithPrefix) {
        for (String prefix : WIKIDATA_ENTITY_NAMESPACE_NAMES) {
            if (entityIdWithPrefix.startsWith(prefix + ":")) {
                return entityIdWithPrefix.substring(prefix.length() + 1);
            }
        }
        return entityIdWithPrefix;
    }

    public char getPrefixChar() {
        return prefix.charAt(0);
    }

    public String toString() {
        return prefix + id;
    }

    public static class Serializer extends JsonSerializer<EntityId> {
        @Override
        public void serialize(EntityId value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
            gen.writeString(value.toString());
        }
    }

    public static class Deserializer extends JsonDeserializer<EntityId> {
        @Override
        public EntityId deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            return EntityId.parse(p.getText());
        }
    }
}
