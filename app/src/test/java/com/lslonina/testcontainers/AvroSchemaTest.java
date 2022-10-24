package com.lslonina.testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;
import org.rnorth.ducttape.unreliables.Unreliables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

import com.lslonina.testcontainers.avro.Notification;

import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.serializers.subject.TopicNameStrategy;

public class AvroSchemaTest {
        private static final Logger LOGGER = LoggerFactory.getLogger(AvroSchemaTest.class);

        private static final DockerImageName SCHEMA_REGISTRY_IMAGE = DockerImageName.parse("confluentinc/cp-schema-registry:7.2.2");
        private static final DockerImageName KAFKA_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-server:7.2.2")
                .asCompatibleSubstituteFor("confluentinc/cp-kafka");
        private static final DockerImageName ZOOKEEPER_TEST_IMAGE = DockerImageName.parse("confluentinc/cp-zookeeper:7.2.2");

        private static final String TOPIC = "Notifications";

        @Test
        public void testContainerizedPlatform() throws Exception {
                try (
                                Network network = Network.newNetwork();
                                KafkaContainer kafka = new KafkaContainer(KAFKA_TEST_IMAGE)
                                                .withNetworkAliases("kafka")
                                                .withNetwork(network).withExternalZookeeper("zookeeper:2181");

                                GenericContainer<?> zookeeper = new GenericContainer<>(ZOOKEEPER_TEST_IMAGE)
                                                .withNetwork(network)
                                                .withNetworkAliases("zookeeper")
                                                .withEnv("ZOOKEEPER_CLIENT_PORT", "2181");

                                GenericContainer<?> application = new GenericContainer<>(
                                                DockerImageName.parse("alpine"))
                                                .withNetwork(network)
                                                .withNetworkAliases("dummy")
                                                .withCommand("sleep 10000")) {

                        GenericContainer<?> schemaRegistry = new GenericContainer<>(
                                        SCHEMA_REGISTRY_IMAGE)
                                        .withNetwork(network)
                                        .withNetworkAliases("schema-registry")
                                        .withEnv("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "kafka:9092")
                                        .withEnv("SCHEMA_REGISTRY_HOST_NAME", "localhost")
                                        .withExposedPorts(8081)
                                        .waitingFor(Wait.forHttp("/subjects"));

                        zookeeper.start();
                        kafka.start();
                        application.start();
                        schemaRegistry.start();

                        String bootstrapServers = kafka.getBootstrapServers();
                        String schemaRegistryUrl = getSchemaRegistryUrl(
                                        schemaRegistry.getHost(), schemaRegistry.getMappedPort(8081));

                        createTopics(bootstrapServers);
                        produceNotification(bootstrapServers, schemaRegistryUrl);
                        consumeNotification(bootstrapServers, schemaRegistryUrl);
                }
        }

        private void produceNotification(String bootstrapServers, String schemaRegistryUrl) {
                try (KafkaProducer<String, Notification> kafkaProducer = createKafkaProducer(bootstrapServers,
                                schemaRegistryUrl)) {
                        var notification = new Notification(UUID.randomUUID().toString(), "1234",
                                        Instant.now().toEpochMilli());
                        sendNotification(kafkaProducer, notification);
                }
        }

        private String getSchemaRegistryUrl(String host, Integer mappedPort) {
                return String.format("http://%s:%d", host, mappedPort);
        }

        protected void consumeNotification(String bootstrapServers, String schemaRegistryUrl)
                        throws Exception {

                try (KafkaConsumer<String, Notification> kafkaConsumer = createKafkaConsumer(bootstrapServers,
                                schemaRegistryUrl)) {
                        kafkaConsumer.subscribe(Collections.singletonList(TOPIC));

                        Unreliables.retryUntilTrue(
                                        240,
                                        TimeUnit.SECONDS,
                                        () -> {
                                                ConsumerRecords<String, Notification> records = kafkaConsumer
                                                                .poll(Duration.ofMillis(100));

                                                if (records.isEmpty()) {
                                                        return false;
                                                }

                                                Assertions.assertThat(records).hasSize(1);
                                                Assertions.assertThat(records).singleElement();

                                                System.out.println("Received: " + records.iterator().next().value());
                                                return true;
                                        });

                        kafkaConsumer.unsubscribe();
                }
        }

        private static void createTopics(String bootstrapServers) throws InterruptedException, ExecutionException {
                try (var adminClient = createAdminClient(bootstrapServers)) {
                        short replicationFactor = 1;
                        int partitions = 1;

                        LOGGER.info("Creating topics in Apache Kafka");
                        adminClient.createTopics(
                                        Collections.singletonList(new NewTopic(TOPIC, partitions, replicationFactor)))
                                        .all().get();
                }
        }

        private static AdminClient createAdminClient(String bootstrapServers) {
                var properties = new Properties();
                properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

                return KafkaAdminClient.create(properties);
        }

        private static void sendNotification(KafkaProducer<String, Notification> kafkaProducer,
                        Notification notification) {
                try {
                        final ProducerRecord<String, Notification> record = new ProducerRecord<>(
                                        TOPIC, notification.getNotificationId(), notification);
                        kafkaProducer.send(record);
                        kafkaProducer.flush();
                } catch (final SerializationException e) {
                        LOGGER.error(String.format(
                                        "Serialization exception occurred while trying to send message %s to the topic %s",
                                        notification, TOPIC), e);
                }
        }

        private static KafkaProducer<String, Notification> createKafkaProducer(String bootstrapServers,
                        String schemaRegistryUrl) {
                final Properties props = new Properties();
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ProducerConfig.ACKS_CONFIG, "all");
                props.put(ProducerConfig.RETRIES_CONFIG, 0);
                props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
                props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
                props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
                props.put(KafkaAvroSerializerConfig.VALUE_SUBJECT_NAME_STRATEGY, TopicNameStrategy.class.getName());

                return new KafkaProducer<>(props);
        }

        private static KafkaConsumer<String, Notification> createKafkaConsumer(String bootstrapServers,
                        String schemaRegistryUrl) {
                final Properties props = new Properties();
                props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
                props.put(ConsumerConfig.GROUP_ID_CONFIG, "tc-" + UUID.randomUUID());
                props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
                props.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistryUrl);
                props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
                props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);

                return new KafkaConsumer<>(props);
        }

}
