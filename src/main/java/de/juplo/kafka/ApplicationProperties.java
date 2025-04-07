package de.juplo.kafka;

import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;


@ConfigurationProperties(prefix = "juplo")
@Validated
@Getter
@Setter
public class ApplicationProperties
{
  @NotNull
  @NotEmpty
  private String bootstrapServer;
  @NotNull
  @NotEmpty
  private String clientId;

  @NotNull
  private ConsumerProperties consumer;


  @Validated
  @Getter
  @Setter
  static class ConsumerProperties
  {
    @NotNull
    @NotEmpty
    private String groupId;
    @NotNull
    @NotEmpty
    private String topic;
    @NotNull
    @NotEmpty
    private String headerPrefix;
  }
}
