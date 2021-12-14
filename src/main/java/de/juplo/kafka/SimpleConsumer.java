package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


@Slf4j
public class SimpleConsumer
{
  private long consumed = 0;
  private KafkaConsumer<String, String> consumer;
  private Lock lock = new ReentrantLock();
  private Condition stopped = lock.newCondition();


  public SimpleConsumer()
  {
    // tag::create[]
    Properties props = new Properties();
    props.put("bootstrap.servers", ":9092");
    props.put("group.id", "my-consumer"); // << Used for Offset-Commits
    // end::create[]
    props.put("auto.offset.reset", "earliest");
    // tag::create[]
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", StringDeserializer.class.getName());

    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
    // end::create[]
    this.consumer = consumer;
  }


  public void run()
  {
    String id = "C";

    try
    {
      log.info("{} - Subscribing to topic test", id);
      consumer.subscribe(Arrays.asList("test"));

      // tag::loop[]
      while (true)
      {
        ConsumerRecords<String, String> records =
            consumer.poll(Duration.ofSeconds(1));

        // Do something with the data...
        // end::loop[]
        log.info("{} - Received {} messages", id, records.count());
        for (ConsumerRecord<String, String> record : records)
        {
          consumed++;
          log.info(
              "{} - {}: {}/{} - {}={}",
              id,
              record.offset(),
              record.topic(),
              record.partition(),
              record.key(),
              record.value()
          );
        }
        // tag::loop[]
      }
      // end::loop[]
    }
    catch(WakeupException e)
    {
      log.info("{} - RIIING!", id);
    }
    catch(Exception e)
    {
      log.error("{} - Unexpected error: {}", id, e.toString());
    }
    finally
    {
      this.lock.lock();
      try
      {
        log.info("{} - Closing the KafkaConsumer", id);
        consumer.close();
        log.info("C - DONE!");
        stopped.signal();
      }
      finally
      {
        this.lock.unlock();
        log.info("{}: Consumed {} messages in total, exiting!", id, consumed);
      }
    }
  }


  public static void main(String[] args) throws Exception
  {
    SimpleConsumer instance = new SimpleConsumer();

    Runtime.getRuntime().addShutdownHook(new Thread(() ->
    {
      instance.lock.lock();
      try
      {
        instance.consumer.wakeup();
        instance.stopped.await();
      }
      catch (InterruptedException e)
      {
        log.warn("Interrrupted while waiting for the consumer to stop!", e);
      }
      finally
      {
        instance.lock.unlock();
      }
    }));

    instance.run();
  }
}
