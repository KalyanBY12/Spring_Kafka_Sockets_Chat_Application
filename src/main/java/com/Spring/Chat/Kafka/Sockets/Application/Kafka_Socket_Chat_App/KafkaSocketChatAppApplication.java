package com.Spring.Chat.Kafka.Sockets.Application.Kafka_Socket_Chat_App;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafka;


@EnableKafka
@SpringBootApplication
public class KafkaSocketChatAppApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaSocketChatAppApplication.class, args);
	}

}
