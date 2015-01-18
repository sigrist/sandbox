package com.sigrist.sandbox.kafka;

import java.util.Date;
import java.util.Properties;
import java.util.Random;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class Sandbox implements CommandLineRunner {

	@Override
	public void run(String... args) {
		long events = 100l;//Long.parseLong(args[0]);
        Random rnd = new Random();
		Properties props = new Properties();
		 
		props.put("metadata.broker.list", "192.168.1.111:49154,192.168.1.111:49155,192.168.1.111:49156");
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("partitioner.class", "com.sigrist.sandbox.kafka.SimplePartitioner");
		props.put("request.required.acks", "1");
		 
		ProducerConfig config = new ProducerConfig(props);
		
		Producer<String, String> producer = new Producer<String, String>(config);
		 
        for (long nEvents = 0; nEvents < events; nEvents++) { 
               long runtime = new Date().getTime();  
               String ip = "192.168.2." + rnd.nextInt(255); 
               String msg = "{ site: \"www.example.com\", ip: \""+ip+"\", runtime: "+runtime+"  }";
               // runtime + ",www.example.com," + ip; 
               KeyedMessage<String, String> data = new KeyedMessage<String, String>("page_visits", ip, msg);
               producer.send(data);
        }
        producer.close();
	}

	public static void main(String[] args) throws Exception {
		SpringApplication.run(Sandbox.class, args);
	}
}