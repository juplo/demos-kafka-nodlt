package de.juplo.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;


public record FetchRequest(
  TopicPartition partition,
  long offset,
  CompletableFuture<ConsumerRecord<byte[], byte[]>> future)
{
  @Override
  public String toString()
  {
    return partition + "@" + offset;
  }
}
