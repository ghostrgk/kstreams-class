package ru.curs.homework;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.*;
import ru.curs.homework.configuration.KafkaConfiguration;
import ru.curs.homework.configuration.TopologyConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static ru.curs.counting.model.TopicNames.*;

public class TestTopology {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> betsTopic;
    private TestOutputTopic<String, Long> bettorTopic;
    private TestOutputTopic<String, Long> teamTopic;
    private TestOutputTopic<String, Fraud> fraudTopic;
    private TestInputTopic<String, EventScore> scoreTopic;


    @BeforeEach
    public void setUp() throws IOException {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(
                topology, config.asProperties());
        betsTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());

        bettorTopic = topologyTestDriver.createOutputTopic(BETTOR_TOTAL_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Long.class).deserializer());
        teamTopic = topologyTestDriver.createOutputTopic(TEAM_TOTAL_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Long.class).deserializer());

        scoreTopic = topologyTestDriver.createInputTopic(EVENT_SCORE_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(EventScore.class).serializer());
        fraudTopic = topologyTestDriver.createOutputTopic(FRAUD_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Fraud.class).deserializer());
    }

    @Test
    void testTopology() {
        Bet bet1 = Bet.builder()
                .bettor("Ann")
                .match("Netherlands-Germany")
                .outcome(Outcome.A)
                .amount(100)
                .odds(1.7).build();


        betsTopic.pipeInput(bet1.key(), bet1);

        TestRecord<String, Long> team_record1 = teamTopic.readRecord();
        assertEquals("Netherlands-Germany:A", team_record1.key());
        assertEquals(170L, team_record1.value().longValue());

        TestRecord<String, Long> bettor_record1 = bettorTopic.readRecord();
        assertEquals("Ann", bettor_record1.key());
        assertEquals(170L, bettor_record1.value().longValue());


        Bet bet2 = Bet.builder()
                .bettor("Bob")
                .match("Netherlands-Germany")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.8).build();

        betsTopic.pipeInput(bet2.key(), bet2);
        TestRecord<String, Long> team_record2 = teamTopic.readRecord();
        assertEquals("Netherlands-Germany:H", team_record2.key());
        assertEquals(180L, team_record2.value().longValue());

        TestRecord<String, Long> bettor_record2 = bettorTopic.readRecord();
        assertEquals("Bob", bettor_record2.key());
        assertEquals(180L, bettor_record2.value().longValue());


        Bet bet3 = Bet.builder()
                .bettor("Bob")
                .match("Netherlands-Germany")
                .outcome(Outcome.A)
                .amount(100)
                .odds(1.9).build();

        betsTopic.pipeInput(bet3.key(), bet3);
        TestRecord<String, Long> team_record3 = teamTopic.readRecord();
        assertEquals("Netherlands-Germany:A", team_record3.key());
        assertEquals(170L + 190L, team_record3.value().longValue());

        TestRecord<String, Long> bettor_record3 = bettorTopic.readRecord();
        assertEquals("Bob", bettor_record3.key());
        assertEquals(180L + 190L, bettor_record3.value().longValue());


        long current = System.currentTimeMillis();
        EventScore value = new EventScore("Cyprus-Belgium", new Score().goalHome(), current);
        scoreTopic.pipeInput(value.getEvent(), value);

        Bet value1 = new Bet("Ann", "Cyprus-Belgium", Outcome.A, 1, 1, current - 100);
        betsTopic.pipeInput(value1.key(), value1);
        Bet value2 = new Bet("Bob", "Cyprus-Belgium", Outcome.H, 1, 1, current - 100);
        betsTopic.pipeInput(value2.key(), value2);
        Bet value3 = new Bet("B", "Cyprus-Belgium", Outcome.H, 1, 1, current - 5000);
        betsTopic.pipeInput(value3.key(), value3);

        Fraud expectedFraud = Fraud.builder()
                .bettor("Bob")
                .match("Cyprus-Belgium")
                .outcome(Outcome.H)
                .amount(1)
                .odds(1)
                .lag(100)
                .build();


        assertEquals(expectedFraud, fraudTopic.readValue());
        assertTrue(fraudTopic.isEmpty());
    }
}
