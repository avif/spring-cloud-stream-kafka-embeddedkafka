package com.example.demo;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.KStream;
import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.cloud.stream.messaging.Processor;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.rule.EmbeddedKafkaRule;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.messaging.handler.annotation.SendTo;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.Map;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@SpringBootTest(
		properties = {
				"spring.autoconfigure.exclude=org.springframework.cloud.stream.test.binder.TestSupportBinderAutoConfiguration",
		},
		classes = {
//				DemoApplicationTest.ExampleAppWorking.class
				DemoApplicationTest.ExampleAppNotWorking.class
		}
)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_EACH_TEST_METHOD)
public class DemoApplicationTest {
	public static String INPUT_TOPIC = "so0544in";
	public static String OUTPUT_TOPIC = "so0544out";

	@ClassRule
	public static EmbeddedKafkaRule embeddedKafkaRule = new EmbeddedKafkaRule(1, true, INPUT_TOPIC, OUTPUT_TOPIC);

	public static EmbeddedKafkaBroker embeddedKafka = embeddedKafkaRule.getEmbeddedKafka();

	private static KafkaTemplate<String, String> template;

	@Autowired
	private KafkaProperties properties;

	private static Consumer<String, String> consumer;

	@BeforeClass
	public static void setup() {
		System.setProperty("spring.kafka.bootstrap-servers", embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.streams.binder.brokers", embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.streams.binder.configuration.brokers", embeddedKafka.getBrokersAsString());
		System.setProperty("spring.cloud.stream.kafka.binder.zkNodes", embeddedKafka.getZookeeperConnectionString());
		System.setProperty("server.port","0");
		System.setProperty("spring.jmx.enabled" , "false");

		Map<String, Object> senderProps = KafkaTestUtils.producerProps(embeddedKafka);
		senderProps.put("key.serializer", StringSerializer.class);
		senderProps.put("value.serializer", StringSerializer.class);
		DefaultKafkaProducerFactory<String, String> pf = new DefaultKafkaProducerFactory<>(senderProps);
		template = new KafkaTemplate<>(pf, true);

		Map<String, Object> consumerProps = KafkaTestUtils.consumerProps("group-id", "false", embeddedKafka);
		consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		DefaultKafkaConsumerFactory<String, String> cf = new DefaultKafkaConsumerFactory<>(consumerProps);
		consumer = cf.createConsumer();
		embeddedKafka.consumeFromAnEmbeddedTopic(consumer, OUTPUT_TOPIC);
	}

	@After
	public void tearDown() {
		if (consumer != null){
			consumer.close();
		}
	}

	@Test
	public void testSendReceive() {
		template.send(INPUT_TOPIC, "foo");

		Map<String, Object> configs = properties.buildConsumerProperties();
		configs.put(ConsumerConfig.GROUP_ID_CONFIG, "test0544");
		configs.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		ConsumerRecord<String, String> cr = KafkaTestUtils.getSingleRecord(consumer, OUTPUT_TOPIC);

		System.out.println("Contenu chaine resultat : " + cr.value());

		assertEquals(cr.value(), "FOO");
	}

	@SpringBootApplication
	@EnableBinding(Processor.class)
	public static class ExampleAppWorking {

		public static void main(String[] args) {
			SpringApplication.run(ExampleAppWorking.class, args);
		}

		@StreamListener(Processor.INPUT)
		@SendTo(Processor.OUTPUT)
		public String receive(String in) {
			return in.toUpperCase();
		}
	}

	@SpringBootApplication
	@EnableBinding(MyBinding.class)
	public static class ExampleAppNotWorking {

		public static void main(String[] args) {
			SpringApplication.run(ExampleAppNotWorking.class, args);
		}

		@StreamListener
		@SendTo(MyBinding.OUTPUT)
		public KStream<String, String> toUpperCase (@Input(MyBinding.INPUT) KStream<String, String> in){
			return in.map((key, val) -> KeyValue.pair(key, val.toUpperCase()));
		}
	}

	public interface MyBinding {
		String INPUT = "input";
		String OUTPUT = "output";

		@Input(INPUT)
		KStream<String, String> messagesIn();

		@Input(OUTPUT)
		KStream<String, String> messagesOut();
	}
}
