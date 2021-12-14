package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


@Slf4j
public class SimpleProducer
{
  public static void main(String[] args) throws Exception
  {
    // tag::create[]
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("key.serializer", StringSerializer.class.getName());
    props.put("value.serializer", StringSerializer.class.getName());

    KafkaProducer<String, String> producer = new KafkaProducer<>(props);
    // end::create[]

    String id = "P";
    long i = 0;

    try
    {
      for (; i < 100 ; i++)
      {
        final long time = System.currentTimeMillis();

        final ProducerRecord<String, String> record = new ProducerRecord<>(
            "test",              // Topic
            Long.toString(i%10), // Key
            Long.toString(i)     // Value
        );

        producer.send(record, (metadata, e) ->
        {
          long now = System.currentTimeMillis();
          if (e == null)
          {
            // HANDLE SUCCESS
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
            i,
            record.key(),
            now - time
        );
      }
    }
    finally
    {
      log.info("{}: Closing the KafkaProducer", id);
      producer.close();
      log.info("{}: Exiting!", id);
    }
  }
}
