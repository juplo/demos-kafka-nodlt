FROM eclipse-temurin:21-jre
VOLUME /tmp
COPY target/*.jar /opt/app.jar
COPY target/libs /opt/libs
ENTRYPOINT [ "java", "-jar", "/opt/app.jar" ]
CMD [ "kafka:9092", "test", "my-group", "DCKR" ]
