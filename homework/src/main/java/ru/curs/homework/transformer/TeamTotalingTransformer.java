package ru.curs.homework.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.counting.model.EventScore;
import ru.curs.counting.model.Outcome;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;

public class TeamTotalingTransformer implements
        StatefulTransformer<String, Long,
                Bet, Long,
                String, Long> {

    public static final String STORE_NAME = "team-totalling-store";

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public String storeName() {
        return STORE_NAME;
    }

    @Override
    public Serde<String> storeKeySerde() {
        return Serdes.String();
    }

    @Override
    public Serde<Long> storeValueSerde() {
        return new JsonSerde<>(Long.class);
    }

    @Override
    public KeyValue<String, Long> transform(Bet key, Long value,
                                            KeyValueStore<String, Long> stateStore) {
        if (key.getOutcome() == Outcome.D) {
            return null;
        }

        String[] teams = key.getMatch().split("-");

        String team = key.getOutcome() == Outcome.H ? teams[0] : teams[1];

        long current = Optional.ofNullable(stateStore.get(team)).orElse(0L);
        current += value;
        stateStore.put(team, current);

        return KeyValue.pair(team, current);
    }
}
