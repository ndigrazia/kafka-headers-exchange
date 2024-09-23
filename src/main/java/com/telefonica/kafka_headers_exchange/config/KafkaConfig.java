package com.telefonica.kafka_headers_exchange.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.common.serialization.Serdes;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.telefonica.kafka_headers_exchange.processor.AllHeaderFilter;
import com.telefonica.kafka_headers_exchange.processor.AnyHeaderFilter;
import com.telefonica.kafka_headers_exchange.processor.FilterProcessor;
import com.telefonica.kafka_headers_exchange.processor.FilterStr;
import com.telefonica.kafka_headers_exchange.serde.EventSerde;

import com.telefonica.schemas.EventSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {

    @Value(value = "${spring.kafka.header.filter.keys}")
    private List<String> filterKeys;
    
    @Value(value = "${spring.kafka.header.filter.values}")
    private List<String> filterValues;

    @Value(value = "${spring.kafka.header.filter.match}")
    private String filterType;

    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;

    @Value("${spring.kafka.topic.source}")
    private String source;

    @Value("${spring.kafka.topic.matching}")
    private String topicMatching;

    @Value("${spring.kafka.topic.non-matching}")
    private String topicNonMatching;

    @Value("${spring.application.name}")
    private String appName;

    @Bean
    public StreamsConfig kStreamsConfigs() {
        Map<String, Object> props = new HashMap<>();

        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        return new StreamsConfig(props);
    }

    @Bean
    public KStream<String, EventSchema> kStream(StreamsBuilder builder) {
        KStream<String, EventSchema> stream = builder.stream(source,
            Consumed.with(Serdes.String(), new EventSerde()));
                    
        stream.process(() -> new FilterProcessor(topicMatching, filterKeys, filterValues, getFilterStr(filterType)))
                .to(topicMatching, Produced.with(Serdes.String(), new EventSerde()));

        stream.process(() -> new FilterProcessor(topicNonMatching, filterKeys, filterValues, getFilterStr(filterType), true))
                .to(topicNonMatching, Produced.with(Serdes.String(), new EventSerde()));
        
        return stream;
    }
    
    @Bean
    public FilterStr getFilterStr(String type) {
        if (type.equalsIgnoreCase("all"))
            return new AllHeaderFilter();

       return new AnyHeaderFilter();
    }

}

