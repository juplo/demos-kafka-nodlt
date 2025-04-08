package de.juplo.kafka;

import com.jayway.jsonpath.JsonPath;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.RecordsToDelete;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.FieldSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.resttestclient.autoconfigure.AutoConfigureTestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.juplo.kafka.ApplicationTests.*;
import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "juplo.bootstrap-server=${spring.embedded.kafka.brokers}",
    "juplo.consumer.topic=" + TOPIC,
    "juplo.controller.header-prefix=" + HEADER_PREFIX,
    "spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
    "spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.ByteArraySerializer",
    "logging.level.de.juplo.kafka=TRACE",
  })
@DirtiesContext
@EmbeddedKafka(topics = TOPIC, partitions = NUM_PARTITIONS)
@AutoConfigureTestRestTemplate
public class ApplicationTests
{
  @DisplayName("Application startup")
  @Test
  public void testApplicationStartup()
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/actuator/health", String.class);
    assertThat(response.getStatusCode())
      .isEqualTo(HttpStatusCode.valueOf(HttpStatus.OK.value()));
    assertThat(JsonPath.parse(response.getBody()).read("$.status", String.class))
      .isEqualTo("UP");
  }

  @DisplayName("Existing offset")
  @ParameterizedTest(name = "partition: {0}")
  @FieldSource("PARTITIONS")
  void testExistingOffset(int partition) throws Exception
  {
    List<SendResult<byte[], byte[]>> results = new LinkedList<>();
    for (int i = 0; i < (partition + 1) * 7; i++)
    {
      SendResult<byte[], byte[]> result = send(partition);
      if (i % (partition + 1) == 0)
      {
        results.add(result);
      }
    }

    results.forEach(result -> fetchAndCheck(result));
  }

  private void fetchAndCheck(SendResult<byte[], byte[]> result)
  {
    RecordMetadata recordMetadata = result.getRecordMetadata();
    ResponseEntity<String> response = fetchRecord(recordMetadata);
    check(result, response);
  }

  private ResponseEntity<String> fetchRecord(RecordMetadata recordMetadata)
  {
    return restTemplate.getForEntity(
      "/{partition}/{offset}",
      String.class,
      recordMetadata.partition(),
      recordMetadata.offset());
  }

  private void check(SendResult<byte[], byte[]> result, ResponseEntity<String> response)
  {
    RecordMetadata recordMetadata = result.getRecordMetadata();

    String key = new String(result.getProducerRecord().key());
    String value = new String(result.getProducerRecord().value());

    assertThat(response.getStatusCode())
      .isEqualTo(HttpStatusCode.valueOf(HttpStatus.OK.value()));
    assertThat(response.getHeaders().toSingleValueMap())
      .containsEntry(
        HEADER_PREFIX + DeadLetterController.KEY,
        key);
    assertThat(response.getHeaders().toSingleValueMap())
      .containsEntry(
        HEADER_PREFIX + DeadLetterController.TIMESTAMP,
        Long.toString(recordMetadata.timestamp()));
    assertThat(response.getBody())
      .isEqualTo(value);
  }

  @DisplayName("Not yet existing offset")
  @ParameterizedTest(name = "partition: {0}")
  @FieldSource("PARTITIONS")
  void testNotYetExistingOffset(int partition)
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/{partition}/666", String.class, partition);
    assertThat(response.getStatusCode())
      .isEqualTo(HttpStatusCode.valueOf(HttpStatus.NOT_FOUND.value()));
  }

  @DisplayName("Already deleted offset")
  @ParameterizedTest(name = "partition: {0}")
  @FieldSource("PARTITIONS")
  void testAlreadyDeletedOffset(int partition) throws Exception
  {
    RecordMetadata recordMetadata = send(partition).getRecordMetadata();
    deleteAllRecords(adminClient);
    ResponseEntity<String> response = restTemplate.getForEntity(
      "/{partition}/{offset}",
      String.class,
      recordMetadata.partition(),
      recordMetadata.offset());
    assertThat(response.getStatusCode())
      .isEqualTo(HttpStatusCode.valueOf(HttpStatus.NOT_FOUND.value()));
  }

  @DisplayName("Non-existent partition")
  @ParameterizedTest(name = "partition: {0}")
  @ValueSource(ints = { -1, NUM_PARTITIONS, 66 })
  void testNonExistentPartition(int partition)
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/{partition}/0", String.class, partition);
    assertThat(response.getStatusCode())
      .isEqualTo(HttpStatusCode.valueOf(HttpStatus.BAD_REQUEST.value()));
  }


  static final String TOPIC = "ExampleConsumerTest_TEST";
  static final int NUM_PARTITIONS = 7;
  static final int[] PARTITIONS = IntStream.range(0, NUM_PARTITIONS).toArray();
  static final String HEADER_PREFIX = "X-FOO--";

  @Autowired
  KafkaTemplate<byte[], byte[]> kafkaTemplate;
  @Autowired
  AdminClient adminClient;
  @Autowired
  TestRestTemplate restTemplate;

  final long[] currentOffsets = new long[NUM_PARTITIONS];

  int message = 0;


  @BeforeEach
  void resetCurrentOffsets()
  {
    for (int i = 0; i < NUM_PARTITIONS; i++)
    {
      currentOffsets[i] = -1;
    }
  }

  @AfterEach
  void deleteAllRecords(@Autowired AdminClient adminClient) throws InterruptedException, ExecutionException
  {
    adminClient
      .deleteRecords(recordsToDelete())
      .all()
      .get();
  }

  private Map<TopicPartition, RecordsToDelete> recordsToDelete()
  {
    return IntStream
      .range(0, NUM_PARTITIONS)
      .filter(i -> currentOffsets[i] > -1)
      .mapToObj(i -> Integer.valueOf(i))
      .collect(Collectors.toMap(
        i -> new TopicPartition(TOPIC, i),
        i -> recordsToDelete(i)));
  }

  private RecordsToDelete recordsToDelete(int partition)
  {
    return RecordsToDelete.beforeOffset(currentOffsets[partition] + 1);
  }

  private SendResult<byte[], byte[]> send(int partition) throws Exception
  {
    String key = Integer.toString(partition);
    String value = "Hällö Wöhrld!%? -- " + ++message;
    return kafkaTemplate
      .send(TOPIC, partition, key.getBytes(), value.getBytes())
      .thenApply(result ->
      {
        RecordMetadata metadata = result.getRecordMetadata();
        currentOffsets[metadata.partition()] = metadata.offset();
        return result;
      })
      .get();
  }


  @TestConfiguration
  static class ConsumerRunnableTestConfig
  {
    @Bean
    AdminClient adminClient(@Value("${spring.embedded.kafka.brokers}") String kafkaBroker)
    {
      Map<String, Object> properties = new HashMap<>();
      properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
      return AdminClient.create(properties);
    }
  }
}
