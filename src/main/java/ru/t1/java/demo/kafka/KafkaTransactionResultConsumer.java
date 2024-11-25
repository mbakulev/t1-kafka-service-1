package ru.t1.java.demo.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.t1.java.demo.dto.TransactionAcceptResult;
import ru.t1.java.demo.service.AccountService;
import ru.t1.java.demo.service.TransactionService;

import static java.lang.System.out;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaTransactionResultConsumer {
    private final TransactionService transactionService;
    private final AccountService accountService;
    @KafkaListener(id = "${t1.kafka.consumer.t1_demo_transaction_result}",
            topics = "${t1.kafka.topic.t1_demo_transaction_result}",
            containerFactory = "transactionAcceptResultKafkaListenerContainerFactory")
    public void listener(@Payload TransactionAcceptResult transactionAcceptResult,
                         Acknowledgment ack) {
        log.debug("KafkaTransactionAcceptResultConsumer: Обработка новых сообщений");

        try {
            out.println("TransactionAcceptResult: " + transactionAcceptResult);
            if (transactionAcceptResult.getStatus().equals("ACCEPTED")) {
                transactionService.updateTransactionStatus(transactionAcceptResult.getTransactionId(), "ACCEPTED");
            }
            if (transactionAcceptResult.getStatus().equals("REJECTED")) {
                transactionService.updateTransactionStatus(transactionAcceptResult.getTransactionId(), "REJECTED");

                accountService.changeAccountBalance(
                        transactionAcceptResult.getAccountId(),
                        transactionAcceptResult.getTransactionAmount()
                );
            }
            if (transactionAcceptResult.getStatus().equals("BLOCKED")) {
                transactionService.updateTransactionStatus(transactionAcceptResult.getTransactionId(), "BLOCKED");
                accountService.changeAccountBalance(
                        transactionAcceptResult.getAccountId(),
                        transactionAcceptResult.getTransactionAmount()
                );

                accountService.setFrozenAmount(transactionAcceptResult.getAccountId(), transactionAcceptResult.getTransactionAmount());
            }
        } finally {
            ack.acknowledge();
        }

        log.debug("KafkaTransactionAcceptResultConsumer: записи обработаны");
    }
}
