package com.safeway.ossl.storeconsumer.service;

import com.safeway.ossl.storeconsumer.exception.KafkaPublishException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class DlqReplayServiceTest {

    private static final String TOPIC = "ossl-store-events";
    private static final String DLQ = TOPIC + ".DLQ";
    private static final TopicPartition PARTITION = new TopicPartition(DLQ, 0);

    private ConsumerFactory<String, String> consumerFactory;
    private KafkaTemplate<String, String> kafkaTemplate;
    private MockConsumer<String, String> consumer;
    private final Map<TopicPartition, Long> committed = new HashMap<>();
    private DlqReplayService service;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        consumerFactory = mock(ConsumerFactory.class);
        kafkaTemplate = mock(KafkaTemplate.class);
        consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST) {
            @Override
            public synchronized void commitSync(Map<TopicPartition, OffsetAndMetadata> offsets) {
                offsets.forEach((tp, om) -> committed.put(tp, om.offset()));
                super.commitSync(offsets);
            }
        };
        when(consumerFactory.createConsumer()).thenReturn(consumer);
        service = new DlqReplayService(consumerFactory, kafkaTemplate, TOPIC);
    }

    @SuppressWarnings("unchecked")
    private void sendSucceeds() {
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(mock(SendResult.class)));
    }

    private void dlqContains(String... keysAndValues) {
        int count = keysAndValues.length / 2;
        consumer.updatePartitions(DLQ, List.of(new PartitionInfo(DLQ, 0, null, null, null)));
        consumer.updateBeginningOffsets(Map.of(PARTITION, 0L));
        consumer.updateEndOffsets(Map.of(PARTITION, (long) count));
        consumer.schedulePollTask(() -> {
            for (int i = 0; i < count; i++) {
                consumer.addRecord(new ConsumerRecord<>(DLQ, 0, i, keysAndValues[2 * i], keysAndValues[2 * i + 1]));
            }
        });
    }

    @Test
    @SuppressWarnings("unchecked")
    void replay_republishesEveryDlqMessageWithItsKeyAndCommitsOffsets() {
        sendSucceeds();
        dlqContains("1001", "{\"locationId\":\"1001\"}", "1002", "{\"locationId\":\"1002\"}");

        int replayed = service.replay();

        assertEquals(2, replayed);
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate, times(2)).send(captor.capture());
        assertEquals(TOPIC, captor.getAllValues().get(0).topic());
        assertEquals("1001", captor.getAllValues().get(0).key());
        assertEquals("{\"locationId\":\"1002\"}", captor.getAllValues().get(1).value());
        assertEquals(2L, committed.get(PARTITION));
    }

    @Test
    @SuppressWarnings("unchecked")
    void replay_whenDlqIsEmpty_replaysNothing() {
        dlqContains();

        long start = System.nanoTime();
        assertEquals(0, service.replay());

        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
        // an empty DLQ must return without waiting on poll timeouts
        assertTrue(Duration.ofNanos(System.nanoTime() - start).compareTo(Duration.ofSeconds(1)) < 0);
    }

    @Test
    @SuppressWarnings("unchecked")
    void replay_whenDlqTopicDoesNotExist_replaysNothing() {
        assertEquals(0, service.replay());
        verify(kafkaTemplate, never()).send(any(ProducerRecord.class));
    }

    @Test
    @SuppressWarnings("unchecked")
    void replay_whenRepublishFails_throwsAndDoesNotCommitTheFailedMessage() {
        CompletableFuture<SendResult<String, String>> ok = CompletableFuture.completedFuture(mock(SendResult.class));
        CompletableFuture<SendResult<String, String>> failed = CompletableFuture.failedFuture(new RuntimeException("broker down"));
        when(kafkaTemplate.send(any(ProducerRecord.class))).thenReturn(ok, failed);
        dlqContains("1001", "{}", "1002", "{}");

        assertThrows(KafkaPublishException.class, () -> service.replay());

        // only the first message (offset 0 -> committed 1) was replayed successfully
        assertEquals(1L, committed.get(PARTITION));
    }
}
