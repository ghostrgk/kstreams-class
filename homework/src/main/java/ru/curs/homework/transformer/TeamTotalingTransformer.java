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
        return new JsonSerde<>(Long.class);//Serdes.Long();
    }

    @Override
    public KeyValue<String, Long> transform(Bet key, Long value,
                                            KeyValueStore<String, Long> stateStore) {

        String team = null;
        Outcome outcome = key.getOutcome();
        switch (outcome) {
            case H:
                team = key.getMatch().split("-")[0];
                break;
            case D:
                return null;
            case A:
                team = key.getMatch().split("-")[1];
                break;
        }

        long current = Optional.ofNullable(stateStore.get(team)).orElse(0L);
        current += value;
        stateStore.put(team, current);
        return KeyValue.pair(team, current);
    }
}
