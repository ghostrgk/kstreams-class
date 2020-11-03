package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.homework.transformer.BettorTotalingTransformer;
import ru.curs.homework.transformer.TeamTotalingTransformer;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> input = streamsBuilder.stream(BET_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class)));

        KStream<Bet, Long> counted = input.map(
                (key, value) -> KeyValue.pair(value, Math.round(value.getAmount() * value.getOdds()))
        );
//        ).through("counted", Produced.with(new JsonSerde<>(Bet.class), new JsonSerde<>(Long.class)));

        KStream<String, Long> bettor_total = new BettorTotalingTransformer()
                .transformStream(streamsBuilder, counted);
        bettor_total.to(BETTOR_TOTAL_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Long.class)));


        KStream<String, Long> team_total = new TeamTotalingTransformer()
                .transformStream(streamsBuilder, counted);
        team_total.to(TEAM_TOTAL_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Long.class)));


        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }
}
