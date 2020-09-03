package org.wikidata.query.rdf.tool.change;

import static com.google.common.io.Resources.getResource;
import static com.google.common.io.Resources.toByteArray;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.groupingBy;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.fail;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.wikidata.query.rdf.tool.change.events.ChangeEvent;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository.Uris;

import com.codahale.metrics.MetricRegistry;

/**
 * TODO: ideally should be merged with {@link KafkaPollerUnitTest}.
 */
@RunWith(MockitoJUnitRunner.class)
public class KafkaPollerEventConsumptionUnitTest {

    public static final String CREATE_TOPIC = "mediawiki.revision-create";
    public static final String DELETE_TOPIC = "mediawiki.page-delete";
    public static final String UNDELETE_TOPIC = "mediawiki.page-undelete";
    public static final String CHANGE_TOPIC = "mediawiki.page-properties-change";

    private KafkaPoller poller;
    @Mock
    private KafkaConsumer<String, ChangeEvent> consumer;

    private JsonDeserializer<ChangeEvent> deserializer;

    @Test
    public void receiveValidCreateEvent() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(
                record(CREATE_TOPIC, "create-event.json")
        ), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q123");
        assertThat(change.revision()).isEqualTo(1L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-02-19T13:31:23Z"));
    }

    @Test
    public void receiveRealCreateEvent() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(
                record(CREATE_TOPIC, "create-event-full.json")
        ), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q20672616");
        assertThat(change.revision()).isEqualTo(62295L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-21T16:38:20Z"));
    }

    @Test
    public void receivePageDeleteEvent() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(
                record(DELETE_TOPIC, "page-delete.json")
        ), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q47462581");
        assertThat(change.revision()).isEqualTo(621830L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-19T18:53:59Z"));
    }

    @Test
    public void receivePageUndeleteEvent() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(
            record(UNDELETE_TOPIC, "page-undelete.json")
        ), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q32451604");
        assertThat(change.revision()).isEqualTo(565767L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-21T09:30:46Z"));
    }

    @Ignore("temporarily disabled prop-change for performance reasons")
    @Test
    public void receivePropChangeEvent() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(asList(
                record(CHANGE_TOPIC, "prop-change.json"),
                record(CHANGE_TOPIC, "prop-change-wb.json") // this one will be ignored
        )), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q7359206");
        assertThat(change.revision()).isEqualTo(-1L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-23T01:32:14Z"));
    }

    @Test
    public void receiveClusteredEvents() throws RetryableException, IOException, URISyntaxException {
        initPoller("north", "south");
        when(consumer.poll(anyLong())).thenReturn(records(asList(
                record("north." + CREATE_TOPIC, "create-event-full.json"),
                record("south." + DELETE_TOPIC, "page-delete.json")
        )), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(2);

        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q20672616");
        assertThat(change.revision()).isEqualTo(62295L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-21T16:38:20Z"));

        change = changes.get(1);

        assertThat(change.entityId()).isEqualTo("Q47462581");
        assertThat(change.revision()).isEqualTo(621830L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-01-19T18:53:59Z"));
    }

    @Test
    public void receiveOtherEvents() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(asList(
            record(CREATE_TOPIC, "rc-domain.json"),
            record(CREATE_TOPIC, "create-event.json"),
            record(CREATE_TOPIC, "rc-namespace.json")
        )), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q123");
        assertThat(change.revision()).isEqualTo(1L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-02-19T13:31:23Z"));
    }

    private ConsumerRecord<String, ChangeEvent> record(String topic, String data) throws IOException {
        return new ConsumerRecord<>(topic, 0, 0, null, deserializer.deserialize(topic, load(data)));
    }

    private ConsumerRecords<String, ChangeEvent> records(List<ConsumerRecord<String, ChangeEvent>> records) {
        return new ConsumerRecords<>(records.stream().collect(groupingBy(r -> new TopicPartition(r.topic(), r.partition()))));
    }

    private ConsumerRecords<String, ChangeEvent> records(ConsumerRecord<String, ChangeEvent> record) {
        return records(singletonList(record));
    }

    private void initPoller(String...clusterNames) {
        Uris uris = Uris.fromString("https://acme.test");
        URI root = null;
        try {
            root = uris.builder().build();
        } catch (URISyntaxException e) {
            fail("failed to build UriScheme", e);
        }
        KafkaOffsetsRepository kafkaOffsetsRepository = new RdfKafkaOffsetsRepository(root, null);
        Map<String, Class<? extends ChangeEvent>> topicsToClass = KafkaPoller.clusterNamesAwareTopics(Arrays.asList(clusterNames));
        deserializer = new JsonDeserializer<>(topicsToClass);
        poller = new KafkaPoller(consumer,
                uris, Instant.now(), 100, topicsToClass.keySet(), kafkaOffsetsRepository, true,
                new MetricRegistry());
    }


    private byte[] load(String name) throws IOException {
        String prefix = this.getClass().getPackage().getName().replace(".", "/");
        return toByteArray(getResource(prefix + "/events/" + name));
    }

    @Test
    public void receiveCreateEventWithMs() throws RetryableException, IOException {
        initPoller();
        when(consumer.poll(anyLong())).thenReturn(records(
                record(CREATE_TOPIC, "create-event-ms.json")), ConsumerRecords.empty());
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes).hasSize(1);
        Change change = changes.get(0);

        assertThat(change.entityId()).isEqualTo("Q123");
        assertThat(change.revision()).isEqualTo(5L);
        assertThat(change.timestamp()).isEqualTo(Instant.parse("2018-10-24T00:28:24.1623Z"));
    }
}
