package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


@Slf4j
public class ExampleProducer
{
  private final String id;
  private final String topic;
  private final Producer<String, String> producer;

  private volatile boolean running = true;
  private volatile boolean done = false;
  private long produced = 0;

  public ExampleProducer(
    String broker,
    String topic,
    String clientId)
  {
    Properties props = new Properties();
    props.put("bootstrap.servers", broker);
    props.put("client.id", clientId); // Nur zur Wiedererkennung
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    this.id = clientId;
    this.topic = topic;
    producer = new KafkaProducer<>(props);
  }

  public void run()
  {
    long i = 0;

    try
    {
      for (; running; i++)
      {
        send(Long.toString(i%10), Long.toString(i));
        Thread.sleep(500);
      }
    }
    catch (Exception e)
    {
      log.error("{} - Unexpected error: {}!", id, e.toString());
    }
    finally
    {
      log.info("{}: Closing the KafkaProducer", id);
      producer.close();
      log.info("{}: Produced {} messages in total, exiting!", id, produced);
      done = true;
    }
  }

  void send(String key, String value)
  {
    final long time = System.currentTimeMillis();

    final ProducerRecord<String, String> record = new ProducerRecord<>(
        topic,  // Topic
        key,    // Key
        value   // Value
    );

    producer.send(record, (metadata, e) ->
    {
      long now = System.currentTimeMillis();
      if (e == null)
      {
        // HANDLE SUCCESS
        produced++;
        log.debug(
            "{} - Sent key={} message={} partition={}/{} timestamp={} latency={}ms",
            id,
            record.key(),
            record.value(),
            metadata.partition(),
            metadata.offset(),
            metadata.timestamp(),
            now - time
        );
      }
      else
      {
        // HANDLE ERROR
        log.error(
            "{} - ERROR key={} timestamp={} latency={}ms: {}",
            id,
            record.key(),
            metadata == null ? -1 : metadata.timestamp(),
            now - time,
            e.toString()
        );
      }
    });

    long now = System.currentTimeMillis();
    log.trace(
        "{} - Queued message with key={} latency={}ms",
        id,
        record.key(),
        now - time
    );
  }


  public static void main(String[] args) throws Exception
  {
    String broker = ":9092";
    String topic = "test";
    String clientId = "DEV";

    switch (args.length)
    {
      case 3:
        clientId = args[2];
      case 2:
        topic = args[1];
      case 1:
        broker = args[0];
    }

    ExampleProducer instance = new ExampleProducer(broker, topic, clientId);

    Runtime.getRuntime().addShutdownHook(new Thread(() ->
    {
      instance.running = false;
      while (!instance.done)
      {
        log.info("Waiting for main-thread...");
        try
        {
          Thread.sleep(1000);
        }
        catch (InterruptedException e) {}
      }
      log.info("Shutdown completed.");
    }));

    log.info(
        "Running ExampleProducer: broker={}, topic={}, client-id={}",
        broker,
        topic,
        clientId);
    instance.run();
  }
}
