package com.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.entity.Book;
import com.kafka.entity.LibraryEvent;
import com.kafka.entity.LibraryEventType;
import com.kafka.jpa.LibraryEventsRepository;
import com.kafka.service.LibraryEventsService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.ContainerTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.*;

@SpringBootTest
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.consumer.bootstrap-servers=${spring.embedded.kafka.brokers}"})
public class LibraryEventsConsumerIntegrationTest {

    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;

    @Autowired
    KafkaTemplate<Integer, String> kafkaTemplate;

    @Autowired
    KafkaListenerEndpointRegistry kafkaListenerEndpointRegistry;

    @SpyBean
    LibraryEventsConsumer libraryEventsConsumerSpy;

    @SpyBean
    LibraryEventsService libraryEventsServiceSpy;

    @Autowired
    LibraryEventsRepository libraryEventsRepository;

    @Autowired
    ObjectMapper objectMapper;

    @BeforeEach
    void setUp() {
        for(MessageListenerContainer messageListenerContainer: kafkaListenerEndpointRegistry.getListenerContainers()){
            ContainerTestUtils.waitForAssignment(messageListenerContainer, embeddedKafkaBroker.getPartitionsPerTopic());
        }
    }

    @AfterEach
    void tearDown() {
        libraryEventsRepository.deleteAll();;
    }

    @Test
    void publishNewLibraryEvent() throws InterruptedException, ExecutionException, JsonProcessingException {
        String json ="{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Suman\"}}";
        kafkaTemplate.sendDefault(json).get();
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);
        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
        List<LibraryEvent> libraryEventList = (List<LibraryEvent>)libraryEventsRepository.findAll();
        assert  libraryEventList.size() == 1;
        libraryEventList.forEach(libraryEvent -> {
           assert libraryEvent.getLibraryEventId() != null;
           assertEquals(123, libraryEvent.getBook().getBookId());
        });
    }

    @Test
    void publishUpdateLibraryEvent() throws InterruptedException, ExecutionException, JsonProcessingException {
        String json ="{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"Kafka using Spring Boot\",\"bookAuthor\":\"Suman\"}}";
        LibraryEvent libraryEvent = objectMapper.readValue(json, LibraryEvent.class);
        libraryEvent.getBook().setLibraryEvent(libraryEvent);
        libraryEventsRepository.save(libraryEvent);

        Book updatedBook = Book.builder()
                .bookId(123).bookName("Kafka using Spring Boot 2.x").bookAuthor("Suman").build();
        libraryEvent.setLibraryEventType(LibraryEventType.UPDATE);
        libraryEvent.setBook(updatedBook);
        String updatedJson = objectMapper.writeValueAsString(libraryEvent);
        kafkaTemplate.sendDefault(libraryEvent.getLibraryEventId(), updatedJson).get();
        // Wait
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        LibraryEvent persistedLibraryEvent = (LibraryEvent) libraryEventsRepository
                .findById(libraryEvent.getLibraryEventId()).get();
        assertEquals("Kafka using Spring Boot 2.x", persistedLibraryEvent.getBook().getBookName());
    }

    @Test
    void publishModifyLibraryEvent_Not_A_Valid_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 123;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        System.out.println(json);
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));

        Optional<LibraryEvent> libraryEventOptional = libraryEventsRepository.findById(libraryEventId);
        assertFalse(libraryEventOptional.isPresent());
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);


        verify(libraryEventsConsumerSpy, atLeast(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, atLeast(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId_Retry() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_Null_LibraryEventId_Retry_CustomError() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = null;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(1)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_000_LibraryEventId_Retry() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsConsumerSpy, times(3)).onMessage(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(3)).processLibraryEvent(isA(ConsumerRecord.class));
    }

    @Test
    void publishModifyLibraryEvent_000_LibraryEventId_Recovery() throws JsonProcessingException, InterruptedException, ExecutionException {
        //given
        Integer libraryEventId = 000;
        String json = "{\"libraryEventId\":" + libraryEventId + ",\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"Kafka Using Spring Boot\",\"bookAuthor\":\"Dilip\"}}";
        kafkaTemplate.sendDefault(libraryEventId, json).get();
        //when
        CountDownLatch latch = new CountDownLatch(1);
        latch.await(3, TimeUnit.SECONDS);

        verify(libraryEventsServiceSpy, times(4)).processLibraryEvent(isA(ConsumerRecord.class));
        verify(libraryEventsServiceSpy, times(1)).handleRecovery(isA(ConsumerRecord.class));
    }

}
