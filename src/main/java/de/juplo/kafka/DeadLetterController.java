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
  public final static String KEY = "KEY";
  public final static String TIMESTAMP = "TIMESTAMP";


  private final DeadLetterConsumer deadLetterConsumer;
  private final MediaType mediaType;
  private final String headerPrefix;



  public DeadLetterController(
    DeadLetterConsumer deadLetterConsumer,
    ApplicationProperties properties)
  {
    this.deadLetterConsumer = deadLetterConsumer;
    this.mediaType = properties.getController().getMediaType();
    this.headerPrefix = properties.getController().getHeaderPrefix();
  }


  @GetMapping(path = "/{partition}/{offset}")
  public Mono<ResponseEntity<byte[]>> recordAtOffset(
    @PathVariable int partition,
    @PathVariable long offset)
  {
    return deadLetterConsumer
      .requestRecord(partition, offset)
      .map(record -> ResponseEntity
        .ok()
        .contentType(mediaType)
        .header(
          prefixed(KEY),
          UriUtils.encodePathSegment(new String(record.key()), StandardCharsets.UTF_8))
        .header(
          prefixed(TIMESTAMP),
          Long.toString(record.timestamp()))
        .body(record.value()));
  }

  String prefixed(String headerName)
  {
    return headerPrefix + headerName;
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
