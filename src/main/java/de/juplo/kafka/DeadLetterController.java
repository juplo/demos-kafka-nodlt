package de.juplo.kafka;

import de.juplo.kafka.exceptions.NonExistentPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;


@RestController
public class DeadLetterController
{
  @Autowired
  DeadLetterConsumer deadLetterConsumer;


  @GetMapping(path = "/{partition}/{offset}")
  public Mono<ResponseEntity<String>> recordAtOffset(
    @PathVariable int partition,
    @PathVariable long offset)
  {
    return deadLetterConsumer
      .requestRecord(partition, offset)
      .map(record -> ResponseEntity
        .ok()
        .header(
          deadLetterConsumer.prefixed(DeadLetterConsumer.KEY),
          UriUtils.encodePathSegment(record.key(), StandardCharsets.UTF_8))
        .header(
          deadLetterConsumer.prefixed(DeadLetterConsumer.TIMESTAMP),
          Long.toString(record.timestamp()))
        .body(record.value()));
  }

  @ResponseStatus(value= HttpStatus.NOT_FOUND)
  @ExceptionHandler(OffsetOutOfRangeException.class)
  public void notFound(OffsetOutOfRangeException e)
  {
  }

  @ResponseStatus(value= HttpStatus.BAD_REQUEST)
  @ExceptionHandler(NonExistentPartitionException.class)
  public void badRequest(NonExistentPartitionException e)
  {
  }
}
