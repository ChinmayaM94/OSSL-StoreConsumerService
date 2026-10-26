package com.safeway.ossl.storeconsumer.listener;

import com.safeway.ossl.storeconsumer.document.StoreDocument;
import com.safeway.ossl.storeconsumer.dto.StoreChangeEvent;
import com.safeway.ossl.storeconsumer.dto.StoreRecord;
import com.safeway.ossl.storeconsumer.mapper.StoreMapper;
import com.safeway.ossl.storeconsumer.repository.StoreMongoRepository;
import com.safeway.ossl.storeconsumer.service.ChangeEventProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Optional;

/**
 * Consumes store events and upserts them into MongoDB (version-guarded), then publishes a change event
 * so query-side services can refresh their caches.
 */
@Component
public class StoreEventConsumer {

    private static final Logger log = LoggerFactory.getLogger(StoreEventConsumer.class);

    private final StoreMongoRepository mongoRepository;
    private final ChangeEventProducerService changeEventProducerService;

    public StoreEventConsumer(StoreMongoRepository mongoRepository,
                              ChangeEventProducerService changeEventProducerService) {
        this.mongoRepository = mongoRepository;
        this.changeEventProducerService = changeEventProducerService;
    }

    @KafkaListener(
            topics = "${kafka.topic.store-events}",
            groupId = "${kafka.consumer.mongo-group-id}",
            containerFactory = "storeRecordKafkaListenerContainerFactory")
    public void onStorePublished(StoreRecord record) {
        if (record == null || record.getLocationId() == null || record.getLocationId().isBlank()) {
            log.warn("Received null/invalid store event message, skipping: {}", record);
            return;
        }

        long incomingVersion = record.getVersion() == null ? 0L : record.getVersion();

        Optional<StoreDocument> existing = mongoRepository.findByLocationId(record.getLocationId());

        boolean shouldApply = existing.isEmpty()
                || existing.get().getVersion() == null
                || incomingVersion > existing.get().getVersion();

        if (!shouldApply) {
            log.info("Skipping out-of-order/duplicate store event for storeId={} incomingVersion={} storedVersion={}",
                    record.getLocationId(), incomingVersion, existing.get().getVersion());
            return;
        }

        try {
            StoreDocument document = StoreMapper.toDocument(record);
            existing.ifPresent(found -> document.setId(found.getId()));

            mongoRepository.save(document);

            log.info("Upserted storeId={} into MongoDB at version={}", record.getLocationId(), incomingVersion);

            changeEventProducerService.publish(
                    new StoreChangeEvent(record.getLocationId(), incomingVersion, Instant.now()));

        } catch (Exception e) {
            log.error("Failed to process store event message for storeId={}: {}",
                    record.getLocationId(), e.getMessage(), e);
            throw e;
        }
    }
}