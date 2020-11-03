package ru.curs.homework;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Outcome;
import ru.curs.homework.configuration.KafkaConfiguration;
import ru.curs.homework.configuration.TopologyConfiguration;
import ru.curs.counting.model.Bet;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static ru.curs.counting.model.TopicNames.*;

public class TestTopology {

    private TopologyTestDriver topologyTestDriver;
    private TestInputTopic<String, Bet> inputTopic;
    private TestOutputTopic<String, Long> bettorTopic;
    private TestOutputTopic<String, Long> teamTopic;

    @BeforeEach
    public void setUp() throws IOException {
        KafkaStreamsConfiguration config = new KafkaConfiguration().getStreamsConfig();
        StreamsBuilder sb = new StreamsBuilder();
        Topology topology = new TopologyConfiguration().createTopology(sb);
        topologyTestDriver = new TopologyTestDriver(
                topology, config.asProperties());
        inputTopic = topologyTestDriver.createInputTopic(BET_TOPIC, Serdes.String().serializer(),
                new JsonSerde<>(Bet.class).serializer());

        bettorTopic = topologyTestDriver.createOutputTopic(BETTOR_TOTAL_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Long.class).deserializer());
        teamTopic = topologyTestDriver.createOutputTopic(TEAM_TOTAL_TOPIC, Serdes.String().deserializer(),
                new JsonSerde<>(Long.class).deserializer());
    }


    @Test
    void testTopology() {
        Bet bet1 = Bet.builder()
                .bettor("BA")
                .match("TA-TB")
                .outcome(Outcome.A)
                .amount(100)
                .odds(1.7).build();


        inputTopic.pipeInput(bet1.key(), bet1);

        TestRecord<String, Long> team_record1 = teamTopic.readRecord();
        assertEquals("TB", team_record1.key());
        assertEquals(170L, team_record1.value().longValue());

        TestRecord<String, Long> bettor_record1 = bettorTopic.readRecord();
        assertEquals("BA", bettor_record1.key());
        assertEquals(170L, bettor_record1.value().longValue());


        Bet bet2 = Bet.builder()
                .bettor("BB")
                .match("TA-TB")
                .outcome(Outcome.H)
                .amount(100)
                .odds(1.8).build();

        inputTopic.pipeInput(bet2.key(), bet2);
        TestRecord<String, Long> team_record2 = teamTopic.readRecord();
        assertEquals("TA", team_record2.key());
        assertEquals(180L, team_record2.value().longValue());

        TestRecord<String, Long> bettor_record2 = bettorTopic.readRecord();
        assertEquals("BB", bettor_record2.key());
        assertEquals(180L, bettor_record2.value().longValue());


        Bet bet3 = Bet.builder()
                .bettor("BB")
                .match("TA-TB")
                .outcome(Outcome.A)
                .amount(100)
                .odds(1.9).build();

        inputTopic.pipeInput(bet3.key(), bet3);
        TestRecord<String, Long> team_record3 = teamTopic.readRecord();
        assertEquals("TB", team_record3.key());
        assertEquals(170L + 190L, team_record3.value().longValue());

        TestRecord<String, Long> bettor_record3 = bettorTopic.readRecord();
        assertEquals("BB", bettor_record3.key());
        assertEquals(180L + 190L, bettor_record3.value().longValue());
    }
}
