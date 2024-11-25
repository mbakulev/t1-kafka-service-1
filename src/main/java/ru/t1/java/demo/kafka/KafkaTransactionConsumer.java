package ru.t1.java.demo.kafka;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;
import ru.t1.java.demo.dto.TransactionAccept;
import ru.t1.java.demo.dto.TransactionDto;
import ru.t1.java.demo.model.Account;
import ru.t1.java.demo.model.Transaction;
import ru.t1.java.demo.service.AccountService;
import ru.t1.java.demo.service.TransactionService;

import java.util.List;
import java.util.UUID;

import static java.lang.System.out;

@Slf4j
@RequiredArgsConstructor
@Component
public class KafkaTransactionConsumer {

    private final TransactionService transactionService;
    private final AccountService accountService;
    private final KafkaTemplate<String, TransactionAccept> kafkaTemplate;

    @KafkaListener(id = "${t1.kafka.consumer.transaction-group-id}",
            topics = "${t1.kafka.topic.client_transactions}",
            containerFactory = "transactionKafkaListenerContainerFactory")
    public void listener(@Payload TransactionDto transactionDtos,
                         Acknowledgment ack) {
        log.debug("KafkaTransactionConsumer: Обработка новых сообщений");

        try {
            out.println("received dto " + transactionDtos);
            out.println(accountService.checkAccountStatus(transactionDtos.getAccountId()));
            if (accountService.checkAccountStatus(transactionDtos.getAccountId()).equals("OPEN")) {
                Transaction transaction = transactionService.saveTransactionWithStatus(transactionDtos, "REQUESTED");
                Account account = accountService.changeAccountBalance(transactionDtos.getAccountId(), transactionDtos.getAmount());

                out.println("QQQQQQQ");
                TransactionAccept transactionAccept = TransactionAccept.builder()
                        .transactionId(transaction.getId())
                        .transactionAmount(transaction.getAmount())
                        .dateTime(transaction.getDateTime())
                        .accountId(transaction.getAccountId())
                        .userId(account.getClientId())
                        .accountBalance(account.getBalance())
                        .build();

                out.println(transactionAccept);
                kafkaTemplate.send("t1_demo_transaction_accept", UUID.randomUUID().toString(), transactionAccept);
                kafkaTemplate.flush();
            }
            //transactionDtos.forEach(out::println);
            // transactionDtos.forEach(transactionService::saveTransaction);
        } finally {
            ack.acknowledge();
        }

        log.debug("KafkaTransactionConsumer: записи обработаны");
    }
}
