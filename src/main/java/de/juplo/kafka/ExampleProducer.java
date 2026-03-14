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
      log.error("{} - Unexpected error!", id, e);
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
    final long sendRequested = System.currentTimeMillis();

    final ProducerRecord<String, String> record = new ProducerRecord<>(
      topic,  // Topic
      key,    // Key
      value   // Value
    );

    producer.send(record, (metadata, e) ->
    {
      long sendRequestProcessed = System.currentTimeMillis();
      if (e == null)
      {
        // HANDLE SUCCESS
        log.debug(
          "{} - Sent message {}={}, partition={}, offset={}, timestamp={}, latency={}ms",
          id,
          key,
          value,
          metadata.partition(),
          metadata.offset(),
          metadata.timestamp(),
          sendRequestProcessed - sendRequested
        );
      }
      else
      {
        // HANDLE ERROR
        log.error(
          "{} - ERROR for message {}={}, latency={}ms: {}",
          id,
          key,
          value,
          sendRequestProcessed - sendRequested,
          e.toString()
        );
      }
    });

    long sendRequestQueued = System.currentTimeMillis();
    produced++;
    log.trace(
      "{} - Queued message {}={}, latency={}ms",
      id,
      key,
      value,
      sendRequestQueued - sendRequested
    );
  }


  public static void main(String[] args) throws Exception
  {
    if (args.length != 3)
    {
      log.error("Three arguments required!");
      log.error("arg[0]: Broker-Address");
      log.error("arg[1]: Topic");
      log.error("arg[2]: Unique Client-ID");
      System.exit(1);
      return;
    }

    log.info(
      "Running ExampleProducer: broker={}, topic={}, client-id={}",
      args[0],
      args[1],
      args[2]);

    ExampleProducer instance = new ExampleProducer(args[0], args[1], args[2]);

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

    instance.run();
  }
}
