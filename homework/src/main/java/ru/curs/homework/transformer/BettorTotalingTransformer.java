package ru.curs.homework.transformer;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueStore;
import org.springframework.kafka.support.serializer.JsonSerde;
import ru.curs.counting.model.Bet;
import ru.curs.homework.util.StatefulTransformer;

import java.util.Optional;

public class BettorTotalingTransformer implements
        StatefulTransformer<String, Long,
                Bet, Long,
                String, Long> {

    public static final String STORE_NAME = "bettor-totalling-store";

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
        assert value >= 0;

        String bettor = key.getBettor();
        long current = Optional.ofNullable(stateStore.get(bettor)).orElse(0L);
        current += value;
        stateStore.put(bettor, current);
        return KeyValue.pair(bettor, current);
    }
}
