package de.juplo.kafka;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.webmvc.test.autoconfigure.AutoConfigureMockMvc;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.web.servlet.MockMvc;

import java.time.Duration;

import static de.juplo.kafka.ApplicationTests.PARTITIONS;
import static de.juplo.kafka.ApplicationTests.TOPIC;
import static org.awaitility.Awaitility.await;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;


@SpringBootTest(
  properties = {
    "juplo.bootstrap-server=${spring.embedded.kafka.brokers}",
    "juplo.consumer.topic=" + TOPIC })
@AutoConfigureMockMvc
@EmbeddedKafka(topics = TOPIC, partitions = PARTITIONS)
public class ApplicationTests
{
  static final String TOPIC = "FOO";
  static final int PARTITIONS = 10;

  @Autowired
  MockMvc mockMvc;



  @Test
  public void testApplicationStartup()
  {
    await("Application is healthy")
      .atMost(Duration.ofSeconds(5))
      .untilAsserted(() -> mockMvc
        .perform(get("/actuator/health"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("status").value("UP")));
  }
}
