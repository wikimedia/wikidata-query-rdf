package org.wikidata.query.rdf.tool.wikibase;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_NULL;

import java.util.Map;

import javax.annotation.Nullable;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

public class EditRequest {

    @JsonProperty @Nullable @JsonInclude(NON_NULL)
    private final String datatype;
    @JsonProperty
    private final Map<String, Label> labels;

    public EditRequest(String datatype, Map<String, Label> labels) {
        this.datatype = datatype;
        this.labels = labels;
    }

    public String getDatatype() {
        return datatype;
    }

    public Map<String, Label> getLabels() {
        return labels;
    }

    public static class Label {
        @JsonProperty
        private final String language;
        @JsonProperty
        private final String value;

        public Label(String language, String value) {
            this.language = language;
            this.value = value;
        }

        public String getLanguage() {
            return language;
        }

        public String getValue() {
            return value;
        }
    }
}
