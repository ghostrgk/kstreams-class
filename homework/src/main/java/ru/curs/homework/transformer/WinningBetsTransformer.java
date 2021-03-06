package ru.curs.homework.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.counting.model.Score;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;

public class WinningBetsTransformer implements StatefulTransformer<String, Score,
        String, EventScore, String, Bet> {
    @Override
    public String storeName() {
        return "winning-bets-store";
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Score> storeValueSerde() {
        return new JsonSerde<>(Score.class);
    }

    @Override
    public KeyValue<String, Bet> transform(String key, EventScore value, KeyValueStore<String, Score> stateStore) {
        Score current = Optional.ofNullable(stateStore.get(key)).orElse(new Score());
        stateStore.put(key, value.getScore());
        Outcome currentOutcome = value.getScore().getHome() > current.getHome() ?
                Outcome.H : Outcome.A;
        Bet bestBet = new Bet(null, value.getEvent(), currentOutcome, 0, 1, value.getTimestamp());
        return KeyValue.pair(String.format("%s:%s", key, currentOutcome), bestBet);

    }
}