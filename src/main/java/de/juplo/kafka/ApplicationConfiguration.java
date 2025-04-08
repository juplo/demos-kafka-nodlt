package de.juplo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Properties;


@Configuration
@EnableConfigurationProperties(ApplicationProperties.class)
public class ApplicationConfiguration
{
  @Bean
  public DeadLetterConsumer deadLetterConsumer(
    Consumer<byte[], byte[]> kafkaConsumer,
    ApplicationProperties properties,
    ConfigurableApplicationContext applicationContext)
  {
    return
      new DeadLetterConsumer(
        properties.getClientId(),
        properties.getConsumer().getTopic(),
        kafkaConsumer,
        () -> applicationContext.close());
  }

  @Bean(destroyMethod = "")
  public KafkaConsumer<byte[], byte[]> kafkaConsumer(ApplicationProperties properties)
  {
    Properties props = new Properties();

    props.put("bootstrap.servers", properties.getBootstrapServer());
    props.put("client.id", properties.getClientId());
    props.put("group.id", properties.getConsumer().getGroupId());
    props.put("key.deserializer", ByteArrayDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());
    props.put("enable.auto.commit", false);
    props.put("auto.offset.reset", "none");
    props.put("fetch.max.wait.ms", 0);

    return new KafkaConsumer<>(props);
  }
}
