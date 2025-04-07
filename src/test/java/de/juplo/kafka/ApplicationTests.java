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
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static de.juplo.kafka.ApplicationTests.NUM_PARTITIONS;
import static de.juplo.kafka.ApplicationTests.TOPIC;
import static org.assertj.core.api.Assertions.assertThat;


@SpringBootTest(
  webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
  properties = {
    "juplo.bootstrap-server=${spring.embedded.kafka.brokers}",
    "juplo.consumer.topic=" + TOPIC,
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
    assertThat(response.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(HttpStatus.OK.value()));
    assertThat(JsonPath.parse(response.getBody()).read("$.status", String.class)).isEqualTo("UP");
  }

  @DisplayName("Not yet existing offset")
  @Test
  void testNotYetExistingOffset()
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/1/66666666666", String.class);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(HttpStatus.NOT_FOUND.value()));
  }

  @DisplayName("Already deleted offset")
  @Test
  void testAlreadyDeletedOffset(@Autowired AdminClient adminClient) throws Exception
  {
    RecordMetadata recordMetadata = send("Hallo", "Welt!");
    deleteAllRecords(adminClient);
    ResponseEntity<String> response = restTemplate.getForEntity(
      "/{partition}/{offset}",
      String.class,
      recordMetadata.partition(),
      recordMetadata.offset());
    assertThat(response.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(HttpStatus.NOT_FOUND.value()));
  }

  @DisplayName("Non-existent partition")
  @ParameterizedTest(name = "partition: {0}")
  @ValueSource(ints = { -1, NUM_PARTITIONS, 66 })
  void testNonExistentPartition(int partition)
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/{partition}/0", String.class, partition);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(HttpStatus.NOT_FOUND.value()));
  }


  static final String TOPIC = "ExampleConsumerTest_TEST";
  static final int NUM_PARTITIONS = 7;

  @Autowired
  KafkaTemplate<String, String> kafkaTemplate;
  @Autowired
  TestRestTemplate restTemplate;

  final long[] currentOffsets = new long[NUM_PARTITIONS];


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

  private RecordMetadata send(String key, String value) throws Exception
  {
    return kafkaTemplate
      .send(TOPIC, key, value)
      .thenApply(result ->
      {
        RecordMetadata metadata = result.getRecordMetadata();
        currentOffsets[metadata.partition()] = metadata.offset();
        return result.getRecordMetadata();
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
