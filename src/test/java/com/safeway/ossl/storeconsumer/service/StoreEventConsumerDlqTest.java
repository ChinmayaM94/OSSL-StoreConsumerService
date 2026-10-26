package com.safeway.ossl.storeconsumer.service;

import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.safeway.ossl.storeconsumer.dto.StoreRecord;
import com.safeway.ossl.storeconsumer.listener.StoreEventConsumer;
import com.safeway.ossl.storeconsumer.repository.StoreMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.dao.DataAccessResourceFailureException;

import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class StoreEventConsumerDlqTest {

    private StoreMongoRepository mongoRepository;
    private ChangeEventProducerService changeEventProducerService;
    private StoreEventConsumer consumer;

    @BeforeEach
    void setUp() {
        mongoRepository = Mockito.mock(StoreMongoRepository.class);
        changeEventProducerService = Mockito.mock(ChangeEventProducerService.class);

        consumer = new StoreEventConsumer(mongoRepository, changeEventProducerService);
    }

    @Test
    void onStorePublished_whenMongoSaveFails_throwsExceptionSoContainerCanRetryOrSendToDlq() {
        ObjectNode payload = JsonNodeFactory.instance.objectNode();
        payload.put("locationId", "1001");

        StoreRecord record = new StoreRecord("1001", payload, 1L);

        when(mongoRepository.findByLocationId("1001")).thenReturn(Optional.empty());
        when(mongoRepository.save(any())).thenThrow(new DataAccessResourceFailureException("simulated mongo failure"));

        assertThrows(DataAccessResourceFailureException.class, () -> consumer.onStorePublished(record));

        verify(mongoRepository).findByLocationId("1001");
        verify(mongoRepository).save(any());
        verify(changeEventProducerService, never()).publish(any());
    }
}