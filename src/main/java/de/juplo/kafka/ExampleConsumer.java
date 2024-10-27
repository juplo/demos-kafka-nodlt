package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.time.Duration;
import java.util.Arrays;


@Slf4j
public class ExampleConsumer implements Runnable
{
  private final String id;
  private final String topic;
  private final Consumer<String, String> consumer;
  private final Thread workerThread;
  private final Runnable closeCallback;

  private volatile boolean running = false;
  private long consumed = 0;


  public ExampleConsumer(
    String clientId,
    String topic,
    Consumer<String, String> consumer,
    Runnable closeCallback)
  {
    this.id = clientId;
    this.topic = topic;
    this.consumer = consumer;

    workerThread = new Thread(this, "ExampleConsumer Worker-Thread");
    workerThread.start();

    this.closeCallback = closeCallback;
  }


  @Override
  public void run()
  {
    try
    {
      log.info("{} - Subscribing to topic {}", id, topic);
      consumer.subscribe(Arrays.asList(topic));
      running = true;

      while (running)
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
      log.info("{} - Triggering exit of application!", id);
      new Thread(closeCallback).start();
    }
    finally
    {
      log.info("{} - Closing the KafkaConsumer", id);
      consumer.close();
      log.info("{}: Consumed {} messages in total, exiting!", id, consumed);
      running = false;
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


  public void shutdown() throws InterruptedException
  {
    log.info("{} joining the worker-thread...", id);
    running = false;
    consumer.wakeup();
    workerThread.join();
  }
}
