package ru.t1.java.demo.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaClientProducer {

    private final KafkaTemplate template;

    public void send(Long id) {
        try {
            template.sendDefault(UUID.randomUUID().toString(), id).get();
            template.flush();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void sendMessageWithHeaders(String topic, String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                key,
                value
        );

        // Add headers
        Map<String, String> headers = new HashMap<>();
        headers.put("header-key-1", "header-value-1");
        headers.put("header-key-2", "header-value-2");

        record.headers().add((Header) headers);

        template.send(record);
    }

    public void sendTo(String topic, Object o) {
        try {
            template.send(topic, o).get();
            template.flush();
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

}
