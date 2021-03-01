package com.kafka.config;

import com.kafka.service.LibraryEventsService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryContext;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka
@Slf4j
public class LibraryEventsConsumerConfig {

    @Autowired
    LibraryEventsService libraryEventsService;

    @Bean
    ConcurrentKafkaListenerContainerFactory<?, ?> kafkaListenerContainerFactory(ConcurrentKafkaListenerContainerFactoryConfigurer configurer, ObjectProvider<ConsumerFactory<Object, Object>> kafkaConsumerFactory) {
        KafkaProperties properties = null;
        ConcurrentKafkaListenerContainerFactory<Object, Object> factory = new ConcurrentKafkaListenerContainerFactory();
        configurer.configure(factory, (ConsumerFactory)kafkaConsumerFactory.getIfAvailable(() -> {
            return new DefaultKafkaConsumerFactory(properties.buildConsumerProperties());
        }));
        // Spawns 3 threads from the same application, recommended for non cloud environments
        factory.setConcurrency(3);
        // Manual Acknowledgement Mode
        //factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);

        //Custom Error Handler
        factory.setErrorHandler(((thrownException, data) -> {
            log.info("Exception in Consumer Config is {} and the record is {}", thrownException.getMessage(), data);
        }));

        //Set Retry Options
        factory.setRetryTemplate(retryTemplate());
        //Set Recovery Options
        factory.setRecoveryCallback((context ->{
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                log.info("Inside Recoverable Logic");
                Arrays.asList(context.attributeNames()).forEach(attributeName ->{
                    log.info("Attribute Name is : {}", attributeName);
                    log.info("Attribute Value is : {}", context.getAttribute(attributeName));
                });
                ConsumerRecord<Integer, String> consumerRecord = (ConsumerRecord<Integer, String>) context
                        .getAttribute("record");
                libraryEventsService.handleRecovery(consumerRecord);
            }
            else{
                log.info("Inside Non Recoverable Logic");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        }
                ));
        return factory;
    }

    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolicy());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolicy() {
        //SimpleRetryPolicy simpleRetryPolicy = new SimpleRetryPolicy();
        //simpleRetryPolicy.setMaxAttempts(3);
        Map<Class<? extends Throwable>, Boolean> exceptionList = new HashMap<>();
        exceptionList.put(IllegalArgumentException.class, false);
        exceptionList.put(RecoverableDataAccessException.class, true);
        SimpleRetryPolicy simpleRetryPolicy=new SimpleRetryPolicy(3, exceptionList, true);
        return simpleRetryPolicy;
    }
}
