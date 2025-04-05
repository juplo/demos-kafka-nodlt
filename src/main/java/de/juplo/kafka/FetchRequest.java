package de.juplo.kafka;

import org.apache.kafka.common.TopicPartition;

import java.util.concurrent.CompletableFuture;


public record FetchRequest(
  TopicPartition partition,
  long offset,
  CompletableFuture<String> future)
{
  @Override
  public String toString()
  {
    return partition + "@" + offset;
  }
}
