package com.telefonica.kafka_headers_exchange;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class KafkaHeadersExchangeService {

	public static void main(String[] args) {
		SpringApplication.run(KafkaHeadersExchangeService.class, args);
	}

}
