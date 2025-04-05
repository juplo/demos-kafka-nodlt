package de.juplo.kafka;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Collectors;
import java.util.stream.IntStream;


@Slf4j
public class DeadLetterConsumer implements Runnable
{
  private final String id;
  private final String topic;
  private final int numPartitions;
  private final Queue<FetchRequest>[] pendingFetchRequests;
  private final FetchRequest[] currentFetchRequest;
  private final Consumer<String, String> consumer;
  private final Thread workerThread;
  private final Runnable closeCallback;

  private boolean shutdownIsRequested = false;


  public DeadLetterConsumer(
    String clientId,
    String topic,
    Consumer<String, String> consumer,
    Runnable closeCallback)
  {
    this.id = clientId;
    this.topic = topic;
    this.consumer = consumer;

    numPartitions = consumer.partitionsFor(topic).size();
    pendingFetchRequests = IntStream
      .range(0, numPartitions)
      .mapToObj(info -> new ConcurrentLinkedQueue<String>())
      .toArray(size -> new Queue[size]);
    currentFetchRequest = new FetchRequest[numPartitions];

    workerThread = new Thread(this, clientId + "-worker-thread");
    workerThread.start();

    this.closeCallback = closeCallback;
  }


  @Override
  public void run()
  {
    try
    {
      List<TopicPartition> partitions = IntStream
        .range(0, pendingFetchRequests.length)
        .mapToObj(i -> new TopicPartition(topic, i))
        .peek(partition -> log.info("{} - Assigning to {}", id, partition))
        .collect(Collectors.toList());

      consumer.assign(partitions);
      consumer.pause(partitions);

      // Without this, the first call to poll() triggers an NoOffsetForPartitionException, if the topic is empty
      partitions.forEach(partition -> consumer.seek(partition, 0));

      while (!shutdownIsRequested)
      {
        try
        {
          ConsumerRecords<String, String> records = consumer.poll(Duration.ofMinutes(1));

          log.info("{} - Received {} messages", id, records.count());
          List<TopicPartition> partitionsToPause = new LinkedList<>();
          for (TopicPartition partition : records.partitions())
          {
            for (ConsumerRecord<String, String> record : records.records(partition))
            {
              log.info(
                "{} - fetched partition={}-{}, offset={}: {}",
                id,
                topic,
                record.partition(),
                record.offset(),
                record.key());

              FetchRequest fetchRequest = currentFetchRequest[record.partition()];

              fetchRequest.future().complete(record.value());
              schedulePendingFetchRequest(record.partition()).ifPresentOrElse(
                (nextFetchRequest) -> scheduleFetchRequest(nextFetchRequest),
                () ->
                {
                  log.info("{} - no pending fetch-requests for {}", id, partition);
                  currentFetchRequest[record.partition()] = null;
                  partitionsToPause.add(fetchRequest.partition());
                });

              break;
            }
          }

          consumer.pause(partitionsToPause);
        }
        catch(WakeupException e)
        {
          log.info("{} - Consumer was awakened", id);

          List<TopicPartition> partitionsToResume = new LinkedList<>();

          for (int partition = 0; partition < numPartitions; partition++)
          {
            schedulePendingFetchRequest(partition)
              .map(fetchRequest -> fetchRequest.partition())
              .ifPresent(topicPartition -> partitionsToResume.add(topicPartition));
          }

          consumer.resume(partitionsToResume);
        }
      }
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
      log.info("{} - Exiting!", id);
    }
  }

  private Optional<FetchRequest> schedulePendingFetchRequest(int partition)
  {
    if (currentFetchRequest[partition] == null)
    {
      FetchRequest nextFetchRequest = pendingFetchRequests[partition].poll();
      if (nextFetchRequest != null)
      {
        scheduleFetchRequest(nextFetchRequest);
        return Optional.of(nextFetchRequest);
      }
      else
      {
        log.trace("{} - no pending fetch-request for partition {}.", id, partition);
      }
    }
    else
    {
      log.debug("{} - fetch-request {} is still in progress.", id, currentFetchRequest[partition]);
    }

    return Optional.empty();
  }

  private void scheduleFetchRequest(FetchRequest fetchRequest)
  {
    log.debug("{} - scheduling fetch-request {}.", id, fetchRequest);

    currentFetchRequest[fetchRequest.partition().partition()] = fetchRequest;
    consumer.seek(fetchRequest.partition(), fetchRequest.offset());
  }
  Mono<String> requestRecord(int partition, long offset)
  {
    CompletableFuture<String> future = new CompletableFuture<>();

    FetchRequest fetchRequest = new FetchRequest(
      new TopicPartition(topic, partition),
      offset,
      future);

    pendingFetchRequests[partition].add(fetchRequest);

    log.info(
      "{} - fetch-request for partition={}, offset={}: Waking up consumer!",
      id,
      partition,
      offset);
    consumer.wakeup();

    return Mono.fromFuture(future);
  }

  public void shutdown() throws InterruptedException
  {
    log.info("{} - Requesting shutdown", id);
    shutdownIsRequested = true;
    consumer.wakeup();
    log.info("{} - Joining the worker thread", id);
    workerThread.join();
  }
}
