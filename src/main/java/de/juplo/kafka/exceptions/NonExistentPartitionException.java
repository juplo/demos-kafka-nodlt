package de.juplo.kafka.exceptions;

import lombok.Getter;
import org.apache.kafka.common.TopicPartition;


@Getter
public class NonExistentPartitionException extends RuntimeException
{
  private final TopicPartition partition;


  public NonExistentPartitionException(String topic, int partition)
  {
    this(new TopicPartition(topic, partition));
  }

  public NonExistentPartitionException(TopicPartition partition)
  {
    super("Non-existent partition: " + partition);
    this.partition = partition;
  }
}
