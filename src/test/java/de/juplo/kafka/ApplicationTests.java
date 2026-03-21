package de.juplo.kafka;

import com.jayway.jsonpath.JsonPath;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.resttestclient.TestRestTemplate;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;

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
public class ApplicationTests
{
  @Test
  public void testApplicationStartup()
  {
    ResponseEntity<String> response = restTemplate.getForEntity("/actuator/health", String.class);
    assertThat(response.getStatusCode()).isEqualTo(HttpStatusCode.valueOf(HttpStatus.OK.value()));
    assertThat(JsonPath.parse(response.getBody()).read("$.status", String.class)).isEqualTo("UP");
  }


  static final String TOPIC = "ExampleConsumerTest_TEST";
  static final int NUM_PARTITIONS = 7;

  @Autowired
  TestRestTemplate restTemplate;
}
