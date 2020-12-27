package br.com.alura.ecommerce;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializador dos objetos para inserir no Kafka
 * @param <T>
 */
public class GsonSerializer<T> implements Serializer {
    private final Gson gson = new GsonBuilder().create();

    @Override
    public byte[] serialize(String s, Object o) {
        return gson.toJson(o).getBytes();
    }
}
