package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;


@Slf4j
public class SimpleProducer
{
  private final String id;
  private final String topic;
  private final KafkaProducer<String, String> producer;

  private long produced = 0;

  public SimpleProducer(String clientId, String topic)
  {
    // tag::create[]
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    // end::create[]

    this.id = clientId;
    this.topic = topic;
    this.producer = producer;
  }

  public void run()
  {
    long i = 0;

    try
    {
      for (; i < 100 ; i++)
      {
        send(Long.toString(i%10), Long.toString(i));
      }

      log.info("{} - Done", id);
    }
    finally
    {
      log.info("{}: Closing the KafkaProducer", id);
      producer.close();
      log.info("{}: Produced {} messages in total, exiting!", id, produced);
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
        "{} - Queued #{} key={} latency={}ms",
        id,
        value,
        record.key(),
        now - time
    );
  }


  public static void main(String[] args) throws Exception
  {
    SimpleProducer producer = new SimpleProducer("P", "test");
    producer.run();
  }
}
