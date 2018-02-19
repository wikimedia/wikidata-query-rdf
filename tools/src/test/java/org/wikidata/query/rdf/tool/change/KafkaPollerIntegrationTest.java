package org.wikidata.query.rdf.tool.change;

import static com.google.common.base.Charsets.UTF_8;
import static com.google.common.io.Resources.getResource;
import static java.util.Collections.emptyList;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.text.ParseException;
import java.time.Instant;
import java.util.Collection;
import java.util.List;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExternalResource;
import org.wikidata.query.rdf.tool.exception.RetryableException;
import org.wikidata.query.rdf.tool.wikibase.WikibaseRepository;

import com.github.charithe.kafka.EphemeralKafkaBroker;
import com.github.charithe.kafka.KafkaJunitRule;
import com.google.common.collect.ImmutableList;
import com.google.common.io.Resources;

public class KafkaPollerIntegrationTest {

    public static final String CREATE_TOPIC = "mediawiki.revision-create";
    public static final String DELETE_TOPIC = "mediawiki.page-delete";
    public static final String UNDELETE_TOPIC = "mediawiki.page-undelete";
    public static final String CHANGE_TOPIC = "mediawiki.page-properties-change";

    @ClassRule
    public static KafkaJunitRule kafkaRule = new KafkaJunitRule(EphemeralKafkaBroker.create()).waitForStartup();

    private KafkaPoller poller;

    @Rule
    public final ExternalResource resource = new ExternalResource() {
        @Override
        protected void before() throws Throwable {
            poller = createPoller();
        };

        @Override
        protected void after() {
            poller.close();
        };
    };

    @Test
    public void receiveValidCreateEvent() throws RetryableException, IOException, ParseException {
        sendEvent(CREATE_TOPIC, "create-event.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q123"));
        assertThat(change.revision(), is(1L));
        assertThat(change.timestamp(), is(Instant.parse("2018-02-19T13:31:23Z")));
    }

    @Test
    public void receiveRealCreateEvent() throws RetryableException, IOException, ParseException {
        sendEvent(CREATE_TOPIC, "create-event-full.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q20672616"));
        assertThat(change.revision(), is(62295L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-21T16:38:20Z")));
    }

    @Test
    public void receivePageDeleteEvent() throws RetryableException, IOException, ParseException {
        sendEvent(DELETE_TOPIC, "page-delete.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q47462581"));
        assertThat(change.revision(), is(-1L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-19T18:53:59Z")));
    }

    @Test
    public void receivePageUndeleteEvent() throws RetryableException, IOException, ParseException {
        sendEvent(UNDELETE_TOPIC, "page-undelete.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q32451604"));
        assertThat(change.revision(), is(565767L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-21T09:30:46Z")));
    }

    @Test
    public void receivePropChangeEvent() throws RetryableException, IOException, ParseException {
        sendEvent(CHANGE_TOPIC, "prop-change.json");
        sendEvent(CHANGE_TOPIC, "prop-change-wb.json"); // this one will be ignored
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q7359206"));
        assertThat(change.revision(), is(-1L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-23T01:32:14Z")));
    }

    @Test
    public void receiveClusteredEvents() throws RetryableException, IOException, ParseException {
        poller = createPoller(ImmutableList.of("north", "south"));
        sendEvent("north." + CREATE_TOPIC, "create-event-full.json");
        sendEvent("south." + DELETE_TOPIC, "page-delete.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(2));

        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q20672616"));
        assertThat(change.revision(), is(62295L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-21T16:38:20Z")));

        change = changes.get(1);

        assertThat(change.entityId(), is("Q47462581"));
        assertThat(change.revision(), is(-1L));
        assertThat(change.timestamp(), is(Instant.parse("2018-01-19T18:53:59Z")));
    }

    @Test
    public void receiveOtherEvents() throws RetryableException, IOException, ParseException {
        sendEvent(CREATE_TOPIC, "rc-domain.json");
        sendEvent(CREATE_TOPIC, "create-event.json");
        sendEvent(CREATE_TOPIC, "rc-namespace.json");
        List<Change> changes = poller.firstBatch().changes();

        assertThat(changes, hasSize(1));
        Change change = changes.get(0);

        assertThat(change.entityId(), is("Q123"));
        assertThat(change.revision(), is(1L));
        assertThat(change.timestamp(), is(Instant.parse("2018-02-19T13:31:23Z")));
    }

    private void sendEvent(String topic, String eventFile) throws IOException {
        String eventData = load(eventFile);
        try (KafkaProducer<String, String> producer = kafkaRule.helper().createStringProducer()) {
            producer.send(new ProducerRecord<>(topic, eventData));
        }
    }

    private KafkaPoller createPoller() {
        String servers = "localhost:" + kafkaRule.helper().kafkaPort();
        WikibaseRepository.Uris uris = new WikibaseRepository.Uris("https", "acme.test");
        return KafkaPoller.buildKafkaPoller(servers, emptyList(), uris, 5, Instant.now());
    }

    private KafkaPoller createPoller(Collection<String> clusterNames) {
        String servers = "localhost:" + kafkaRule.helper().kafkaPort();
        WikibaseRepository.Uris uris = new WikibaseRepository.Uris("https", "acme.test");
        return KafkaPoller.buildKafkaPoller(servers, clusterNames, uris, 5, Instant.now());
    }


    private String load(String name) throws IOException {
        String prefix = this.getClass().getPackage().getName().replace(".", "/");
        return Resources.toString(getResource(prefix + "/events/" + name), UTF_8);
    }

}
