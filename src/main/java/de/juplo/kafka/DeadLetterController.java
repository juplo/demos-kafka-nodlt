package de.juplo.kafka;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Mono;


@RestController
public class DeadLetterController
{
  @Autowired
  DeadLetterConsumer deadLetterConsumer;


  @GetMapping(path = "/{partition}/{offset}")
  public Mono<String> recordAtOffset(
    @PathVariable int partition,
    @PathVariable long offset)
  {
    return deadLetterConsumer.requestRecord(partition, offset);
  }
}
