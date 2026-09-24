package com.kafka.saga.PaymentService.service;


import com.kafka.saga.CoreService.dto.Payment;
import com.kafka.saga.PaymentService.entity.PaymentEntity;
import com.kafka.saga.PaymentService.repo.PaymentRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.List;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class PaymentServiceImpl implements PaymentService {
    public static final String SAMPLE_CREDIT_CARD_NUMBER = "374245455400126";
    private final PaymentRepository paymentRepository;
    private final CreditCardProcessorRemoteService ccpRemoteService;

    @Override
    public Payment process(Payment payment) {
        BigDecimal totalPrice = payment.getProductPrice()
                .multiply(new BigDecimal(payment.getProductQuantity()));
        ccpRemoteService.process(new BigInteger(SAMPLE_CREDIT_CARD_NUMBER), totalPrice);
        PaymentEntity paymentEntity = PaymentEntity
                .builder()
                .orderId(payment.getOrderId())
                .productId(payment.getProductId())
                .productQuantity(payment.getProductQuantity())
                .productPrice(payment.getProductPrice())
                .build();
        paymentRepository.save(paymentEntity);

        Payment processedPayment = Payment.builder()
                        .id(paymentEntity.getId())
                        .orderId(payment.getOrderId())
                        .productId(payment.getProductId())
                        .productQuantity(payment.getProductQuantity())
                        .productPrice(payment.getProductPrice())
                        .build();
        return processedPayment;
    }

    @Override
    public List<Payment> findAll() {
        return paymentRepository.findAll().stream().map(entity -> new Payment(entity.getId(), entity.getOrderId(), entity.getProductId(), entity.getProductPrice(), entity.getProductQuantity())
        ).collect(Collectors.toList());
    }
}
