package com.kafka.saga.PaymentService.config;

import com.kafka.saga.CoreService.exception.NonRetryableException;
import com.kafka.saga.CoreService.exception.RetryableException;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

import java.util.Map;

@Configuration
public class PaymentKafkaConfig {


    @Value("${payment.events.topic.name}")
    private String topicName;

    @Value("${spring.kafka.partition.count}")
    private int partitionCount;

    @Value("${spring.kafka.replica.count}")
    private int replicaCount;

    @Value("${spring.kafka.producer.properties.min.insync.replicas}")
    private String minInSyncReplica;

    @Bean
    KafkaTemplate<String, Object> kafkaTemplate(ProducerFactory<String,Object> producerFactory)
    {
        return new KafkaTemplate<String, Object>(producerFactory);
    }


    @Bean
    NewTopic createTopic() {
        return TopicBuilder.name(topicName)
                .partitions(partitionCount)
                .replicas(replicaCount)
                .configs(Map.of("min.insync.replicas", minInSyncReplica))
                .build();
    }



    @Bean
    ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(
            ConsumerFactory<String, Object> consumerFactory, KafkaTemplate<String, Object> kafkaTemplate) {
        DefaultErrorHandler errorHandler = new DefaultErrorHandler(new DeadLetterPublishingRecoverer(kafkaTemplate),new FixedBackOff(3000,3));

        errorHandler.addNotRetryableExceptions(NonRetryableException.class); //Multiple exception can be added comma separated // HttpServerErrorException.class
        errorHandler.addRetryableExceptions(RetryableException.class);

        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<String, Object>();
        factory.setConsumerFactory(consumerFactory);
        factory.setCommonErrorHandler(errorHandler);
        return factory;
    }

}
