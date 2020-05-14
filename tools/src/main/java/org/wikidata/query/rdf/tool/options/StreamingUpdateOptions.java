package org.wikidata.query.rdf.tool.options;

import java.net.URI;
import java.net.URISyntaxException;

import com.lexicalscope.jewel.cli.Option;

public interface StreamingUpdateOptions extends OptionsUtils.BasicOptions {
    @Option(shortName = "b", description = "Kafka brokers to read the stream from")
    String brokers();

    @Option(shortName = "g", description = "Consumer group")
    String consumerGroup();

    @Option(shortName = "t", description = "Topic to read from")
    String topic();

    @Option(shortName = "p", description = "Partition to read from", defaultValue = "0")
    int partition();

    @Option(shortName = "o", description = "Initial offsets to start read from (when no offsets is known)", defaultValue = "earliest")
    String initialOffsets();

    @Option(defaultValue = "1000", description = "Ideal number of RDF triples to batch per update")
    int batchSize();

    @Option(shortName = "u", description = "URL to post updates")
    String sparqlUrl();

    @Option(longName = "metricDomain", defaultValue = "wdqs-streaming-updater", description = "JMX metrics domain")
    String metricDomain();

    static URI sparqlUri(StreamingUpdateOptions options) {
        URI sparqlUri;
        try {
            sparqlUri = new URI(options.sparqlUrl());
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Invalid url:  " + options.sparqlUrl(), e);
        }
        return sparqlUri;
    }
}
