package com.telefonica.kafka_headers_exchange.serde;

import org.apache.kafka.common.serialization.Serdes;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import com.telefonica.schemas.EventSchema;

public class EventSerde extends Serdes.WrapperSerde<EventSchema> {

    public EventSerde() {
        super(new JsonSerializer<>(), new JsonDeserializer<>(EventSchema.class));
    }
    
}

