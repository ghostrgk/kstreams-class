package ru.curs.homework.configuration;

import lombok.RequiredArgsConstructor;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Fraud;
import ru.curs.homework.transformer.BettorTotalingTransformer;
import ru.curs.homework.transformer.TeamTotalingTransformer;
import ru.curs.homework.transformer.WinningBetsTransformer;

import java.time.Duration;

import static ru.curs.counting.model.TopicNames.*;

@Configuration
@RequiredArgsConstructor
public class TopologyConfiguration {
    @Bean
    public Topology createTopology(StreamsBuilder streamsBuilder) {
        KStream<String, Bet> bets = streamsBuilder.stream(BET_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(Bet.class))
                        .withTimestampExtractor((record, previousTimestamp) -> ((Bet) record.value()).getTimestamp()));


        KStream<Bet, Long> counted = bets.map(
                (key, value) -> KeyValue.pair(value, Math.round(value.getAmount() * value.getOdds()))
        );
//        ).through("counted", Produced.with(new JsonSerde<>(Bet.class), new JsonSerde<>(Long.class)));

        KStream<String, Long> bettor_total = new BettorTotalingTransformer()
                .transformStream(streamsBuilder, counted);
        bettor_total.to(BETTOR_TOTAL_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Long.class)));


        KStream<String, Long> team_total = new TeamTotalingTransformer()
                .transformStream(streamsBuilder, counted);
        team_total.to(TEAM_TOTAL_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Long.class)));



        KStream<String, EventScore> eventScores = streamsBuilder.stream(EVENT_SCORE_TOPIC,
                Consumed.with(Serdes.String(), new JsonSerde<>(EventScore.class))
                        .withTimestampExtractor((record, previousTimestamp) ->
                                ((EventScore) record.value()).getTimestamp()));

        KStream<String, Bet> winningBets = new WinningBetsTransformer().transformStream(streamsBuilder, eventScores);

        KStream<String, Fraud> frauds = bets.join(winningBets, (bet, winningBet) ->
                        Fraud.builder()
                                .bettor(bet.getBettor())
                                .outcome(bet.getOutcome())
                                .amount(bet.getAmount())
                                .match(bet.getMatch())
                                .odds(bet.getOdds())
                                .lag(winningBet.getTimestamp() - bet.getTimestamp())
                                .build(),
                JoinWindows.of(Duration.ofSeconds(1)).before(Duration.ZERO),
                StreamJoined.with(Serdes.String(),
                        new JsonSerde<>(Bet.class),
                        new JsonSerde<>(Bet.class)
                ));

        frauds.to(FRAUD_TOPIC, Produced.with(Serdes.String(), new JsonSerde<>(Fraud.class)));

        Topology topology = streamsBuilder.build();
        System.out.println("==============================");
        System.out.println(topology.describe());
        System.out.println("==============================");
        // https://zz85.github.io/kafka-streams-viz/
        return topology;
    }
}
