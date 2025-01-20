package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;


@Slf4j
public class ExampleConsumer
{
  private final String id;
  private final String topic;
  private final Consumer<String, String> consumer;

  private volatile boolean running = false;
  private long consumed = 0;

  public ExampleConsumer(
    String broker,
    String topic,
    String groupId,
    String clientId)
  {
    Properties props = new Properties();
    props.put("bootstrap.servers", broker);
    props.put("group.id", groupId); // ID für die Offset-Commits
    props.put("client.id", clientId); // Nur zur Wiedererkennung
    props.put("auto.offset.reset", "earliest"); // Von Beginn an lesen
    props.put("partition.assignment.strategy", "org.apache.kafka.clients.consumer.CooperativeStickyAssignor");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());
    props.put("metadata.maxage.ms", 5000);

    this.id = clientId;
    this.topic = topic;
    consumer = new KafkaConsumer<>(props);
  }


  public void run()
  {
    try
    {
      log.info("{} - Subscribing to topic {}", id, topic);
      consumer.subscribe(Arrays.asList(topic));
      running = true;

      while (true)
      {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));

        log.info("{} - Received {} messages", id, records.count());
        for (ConsumerRecord<String, String> record : records)
        {
          handleRecord(
            record.topic(),
            record.partition(),
            record.offset(),
            record.key(),
            record.value());
        }
      }
    }
    catch(WakeupException e)
    {
      log.info("{} - Consumer was signaled to finish its work", id);
    }
    catch(Exception e)
    {
      log.error("{} - Unexpected error, unsubscribing!", id, e);
      consumer.unsubscribe();
    }
    finally
    {
      running = false;
      log.info("{} - Closing the KafkaConsumer", id);
      consumer.close();
      log.info("{}: Consumed {} messages in total, exiting!", id, consumed);
    }
  }

  private void handleRecord(
    String topic,
    Integer partition,
    Long offset,
    String key,
    String value)
  {
    consumed++;
    log.info("{} - partition={}-{}, offset={}: {}={}", id, topic, partition, offset, key, value);
  }


  public static void main(String[] args) throws Exception
  {
    String broker = ":9092";
    String topic = "test";
    String groupId = "my-group";
    String clientId = "DEV";

    switch (args.length)
    {
      case 4:
        clientId = args[3];
      case 3:
        groupId = args[2];
      case 2:
        topic = args[1];
      case 1:
        broker = args[0];
    }


    ExampleConsumer instance = new ExampleConsumer(broker, topic, groupId, clientId);

    Runtime.getRuntime().addShutdownHook(new Thread(() ->
    {
      instance.consumer.wakeup();

      while (instance.running)
      {
        log.info("{} - Waiting for main-thread...", instance.id);
        try
        {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {}
      }
      log.info("{} - Shutdown completed.", instance.id);
    }));

    log.info(
      "Running ExampleConsumer: broker={}, topic={}, group-id={}, client-id={}",
      broker,
      topic,
      groupId,
      clientId);
    instance.run();
  }
}
