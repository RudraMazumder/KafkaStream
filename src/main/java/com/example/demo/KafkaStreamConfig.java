package com.example.demo;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;

import io.confluent.developer.User;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDe;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.streams.serdes.avro.GenericAvroSerde;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;

@Configuration
@EnableKafka
@EnableKafkaStreams 
public class KafkaStreamConfig {
	
	@Value("${topic.user.source}")
	private String sourceTopic;
	

	@Value("${topic.user.target}")
	private String targetTopic;

    @Bean(name = 
    KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kStreamsConfigs() {
    	
    	Map<String, Object> map=new HashMap<String, Object>();
    	map.put(StreamsConfig.APPLICATION_ID_CONFIG, "testStreams");
    	map.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:29092");
    	map.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
    	map.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class);
    	map.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8085");
    	
    	return new KafkaStreamsConfiguration(map);
    		
    }
    
    @Bean 
    public KStream <String, User> kStream (StreamsBuilder kStreamBuilder) {
    	
    	
    	
		KStream<String, User> stream = kStreamBuilder.stream(sourceTopic);
		stream
		.filter((t,p)->p.getAge()>10)
		.mapValues((user)->{
			user.setName(user.getName().toUpperCase());
			return user;
		})
		.to(targetTopic);
		
		return stream;
		
    }

}