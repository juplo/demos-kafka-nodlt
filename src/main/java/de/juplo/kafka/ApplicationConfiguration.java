package de.juplo.kafka;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.StickyAssignor;
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
  public ExampleConsumer exampleConsumer(
    Consumer<String, String> kafkaConsumer,
    ApplicationProperties properties,
    ConfigurableApplicationContext applicationContext)
  {
    return
      new ExampleConsumer(
        properties.getClientId(),
        properties.getConsumerProperties().getTopic(),
        kafkaConsumer,
        () -> applicationContext.close());
  }

  @Bean(destroyMethod = "")
  public KafkaConsumer<String, String> kafkaConsumer(ApplicationProperties properties)
  {
    Properties props = new Properties();
    props.put("bootstrap.servers", properties.getBootstrapServer());
    props.put("client.id", properties.getClientId());
    props.put("group.id", properties.getConsumerProperties().getGroupId());
    if (properties.getConsumerProperties().getAutoOffsetReset() != null)
    {
      props.put("auto.offset.reset", properties.getConsumerProperties().getAutoOffsetReset().name());
    }
    if (properties.getConsumerProperties().getAutoCommitInterval() != null)
    {
      props.put("auto.commit.interval", properties.getConsumerProperties().getAutoCommitInterval());
    }
    props.put("metadata.max.age.ms", 5000); //  5 Sekunden
    props.put("partition.assignment.strategy", StickyAssignor.class.getName());
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());

    return new KafkaConsumer<>(props);
  }
}
