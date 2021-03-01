package com.kafka.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.kafka.domain.LibraryEvent;
import com.kafka.domain.LibraryEventType;
import com.kafka.producer.LibraryEventProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.support.SendResult;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

@RestController
@Slf4j
public class LibraryEventController {

    @Autowired
    LibraryEventProducer libraryEventProducer;

    @PostMapping("/v1/libraryevent")
    public ResponseEntity<LibraryEvent> saveLibraryEvent(@RequestBody LibraryEvent libraryEvent) throws JsonProcessingException, InterruptedException, ExecutionException, TimeoutException {
        //invoke kafka producer
        log.info("before sendLibraryEventSynchronous");
        // Approach 1 -- Asynchronous Call
        // libraryEventProducer.sendLibraryEvent(libraryEvent); //Asynchronous call
        // Approach 2 -- Synchronous Call
        //endResult<Integer, String> sendResult = libraryEventProducer.sendLibraryEventSynchronous(libraryEvent);
        //log.info("SendResult is {}", sendResult.toString());
        libraryEvent.setLibraryEventType(LibraryEventType.NEW);
        libraryEventProducer.sendLibraryEventWithTopic(libraryEvent);
        log.info("after sendLibraryEventSynchronous");
        return ResponseEntity.status(HttpStatus.CREATED).body(libraryEvent);
    }
}
