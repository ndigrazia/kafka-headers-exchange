package com.telefonica.kafka_headers_exchange.config;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.TopicNameExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.kafka.common.serialization.Serdes;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.telefonica.kafka_headers_exchange.filter.AllHeaderFilter;
import com.telefonica.kafka_headers_exchange.filter.AnyHeaderFilter;
import com.telefonica.kafka_headers_exchange.filter.FilterStr;
import com.telefonica.kafka_headers_exchange.serde.EventSerde;

import com.telefonica.schemas.EventSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Configuration
public class KafkaConfig {

  private static final Logger logger = LoggerFactory.getLogger(KafkaConfig.class);

  @Value(value = "${spring.kafka.header.filter.keys}")
  private List<String> filterKeys;
  
  @Value(value = "${spring.kafka.header.filter.values}")
  private List<String> filterValues;

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

  @Value(value = "${spring.kafka.header.filter.match}")
  private String filterType;

  @Bean
  public StreamsConfig kStreamsConfigs() {
      Map<String, Object> props = new HashMap<>();

      props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
      props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

      return new StreamsConfig(props);
  }

  @Bean
  public KStream<String, EventSchema> kStream(StreamsBuilder builder) {
    final KStream<String, EventSchema> sourceStream = builder.stream(source,
        Consumed.with(Serdes.String(), new EventSerde()));

    sourceStream.to(topicNameExtractor(), Produced.with(Serdes.String(), new EventSerde()));

    return sourceStream;
  }

  @Bean
  public TopicNameExtractor<String, EventSchema> topicNameExtractor() {
    final TopicNameExtractor<String, EventSchema> topicNameExtractor = (key, value, record) -> {
        final boolean result = filter().applyFilter(filterKeys, filterValues, record.headers());

        final String topic;

        if(result) {
            topic = topicMatching;
        }
        else {
          topic = topicNonMatching;
        }
               
        logger.info("MATCHING [{}], Forwarding record with key: {}, value: {} to Topic: {}.",
            result, key, value, topic);
        
        return topic;
    };

    return topicNameExtractor;
  }

  @Bean
  public FilterStr filter() {
    if ("all".equalsIgnoreCase(filterType)) {
        return new AllHeaderFilter();
    } else if ("any".equalsIgnoreCase(filterType)) {
        return new AnyHeaderFilter();
    }
    
    throw new IllegalArgumentException("Invalid filter type: " + filterType);
  }
 
}

