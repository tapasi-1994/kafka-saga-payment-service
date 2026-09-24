package com.kafka.saga.PaymentService.service;

import com.kafka.saga.CoreService.dto.CreditCardProcessRequest;
import com.kafka.saga.CoreService.exception.CreditCardProcessorUnavailableException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientRequestException;


import java.math.BigDecimal;
import java.math.BigInteger;

@Service
@RequiredArgsConstructor
@Slf4j
public class CreditCardProcessorRemoteServiceImpl implements CreditCardProcessorRemoteService {
    @Value("${remote.ccp.url}")
    private String ccpRemoteServiceUrl;

    private final WebClient webClient;


    @Override
    public void process(BigInteger cardNumber, BigDecimal paymentAmount) {
        try {
            CreditCardProcessRequest request = CreditCardProcessRequest.builder()
                    .creditCardNumber(cardNumber)
                    .paymentAmount(paymentAmount)
                    .build();

            webClient
                    .post()
                    .uri(ccpRemoteServiceUrl + "/ccp/process")
                    .bodyValue(request)
                    .retrieve()
                    .bodyToMono(CreditCardProcessRequest.class)
                    .block();
        } catch (WebClientRequestException e) {

            log.error("Credit Card Processor is unavailable: {}", e.getMessage());

            throw new CreditCardProcessorUnavailableException(
                    "Credit Card Processor is unavailable", e);
        }
    }
}
