package de.juplo.kafka;

import de.juplo.kafka.exceptions.NonExistentPartitionException;
import org.apache.kafka.clients.consumer.OffsetOutOfRangeException;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.util.UriUtils;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;


@RestController
public class DeadLetterController
{
  private final DeadLetterConsumer deadLetterConsumer;
  private final MediaType mediaType;


  public DeadLetterController(
    DeadLetterConsumer deadLetterConsumer,
    ApplicationProperties properties)
  {
    this.deadLetterConsumer = deadLetterConsumer;
    this.mediaType = properties.getController().getMediaType();
  }


  @GetMapping(path = "/{partition}/{offset}")
  public Mono<ResponseEntity<String>> recordAtOffset(
    @PathVariable int partition,
    @PathVariable long offset)
  {
    return deadLetterConsumer
      .requestRecord(partition, offset)
      .map(record -> ResponseEntity
        .ok()
        .contentType(mediaType)
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
