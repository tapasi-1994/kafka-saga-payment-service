package com.kafka.saga.PaymentService.handler;

import com.kafka.saga.CoreService.dto.Payment;
import com.kafka.saga.CoreService.dto.commands.ProcessPaymentCommand;
import com.kafka.saga.CoreService.dto.commands.ReserveProductCommand;
import com.kafka.saga.CoreService.events.PaymentFailedEvent;
import com.kafka.saga.CoreService.events.PaymentProcessedEvent;
import com.kafka.saga.CoreService.exception.CreditCardProcessorUnavailableException;
import com.kafka.saga.PaymentService.service.PaymentService;
import lombok.RequiredArgsConstructor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.messaging.handler.annotation.Payload;

@Configuration
@RequiredArgsConstructor
public class PaymentCommandHandler {
    private static final Logger log = LoggerFactory.getLogger(PaymentCommandHandler.class);
    private final PaymentService paymentService;
    private final KafkaTemplate<String,Object> kafkaTemplate;
    @Value("${payment.events.topic.name}")
    private String paymentEventsTopicName;

    @KafkaListener(
            topics = "${payment.commands.topic.name}",
            groupId = "${spring.kafka.consumer.group-id}"
    )
    public void handleCommand(@Payload ProcessPaymentCommand command) {
        try {
            Payment payment = Payment.builder()
                    .orderId(command.getOrderId())
                    .productId(command.getProductId())
                    .productQuantity(command.getProductQuantity())
                    .productPrice(command.getPrice())
                    .build();

            Payment processedPayment = paymentService.process(payment);
            PaymentProcessedEvent paymentProcessedEvent = PaymentProcessedEvent.builder().orderId(command.getOrderId())
                            .paymentId(processedPayment.getId()).build();
            kafkaTemplate.send(paymentEventsTopicName,paymentProcessedEvent);
        }
        catch (CreditCardProcessorUnavailableException e)
        {
           log.error(e.getMessage());
           PaymentFailedEvent paymentFailedEvent = PaymentFailedEvent.builder()
                    .orderId(command.getOrderId())
                    .productId(command.getProductId())
                    .productQuantity(command.getProductQuantity())
                    .build();
        }
    }

}
