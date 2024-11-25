package ru.t1.java.demo.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import ru.t1.java.demo.dto.TransactionDto;
import ru.t1.java.demo.model.Account;
import ru.t1.java.demo.model.Transaction;
import ru.t1.java.demo.repository.AccountRepository;
import ru.t1.java.demo.repository.TransactionRepository;
import ru.t1.java.demo.service.TransactionService;

import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.UUID;

@Service
@RequiredArgsConstructor
public class TransactionServiceImpl implements TransactionService {

    private final TransactionRepository transactionRepository;
    private final AccountRepository accountRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Override
    public Transaction getTransaction(Long id) {
        return transactionRepository.findById(id).orElse(null);
    }

    @Override
    public void createTransaction(TransactionDto transactionDto) {
        System.out.println("transactionDto: " + transactionDto);
        // System.out.println(transactionDto);
        if (accountRepository.findById(transactionDto.getAccountId()).isPresent()) {
            System.out.println("finded account");
            System.out.println(accountRepository.findById(transactionDto.getAccountId()));
            kafkaTemplate.send("t1_demo_transactions", UUID.randomUUID().toString(), transactionDto);
            kafkaTemplate.flush();
            System.out.println("Transaction sended");
        }
//        Transaction transaction = Transaction.builder()
//                .accountId(transactionDto.getAccountId())
//                .amount(transactionDto.getAmount())
//                .dateTime(LocalDateTime.now())
//                .build();

        // System.out.println("Transaction created: " + transaction.getAccountId() + " " + transaction.getAmount() + " " + transaction.getDateTime());
        // return transactionRepository.save(transaction);
    }

    @Override
    public void saveTransaction(TransactionDto transactionDto) {
        Transaction transaction = Transaction.builder()
                .accountId(transactionDto.getAccountId())
                .amount(transactionDto.getAmount())
                .dateTime(LocalDateTime.now())
                .build();

        System.out.println("Transaction saved: " + transaction.getAccountId() + " " + transaction.getAmount() + " " + transaction.getDateTime());
       transactionRepository.save(transaction);
    }

    @Override
    public Transaction saveTransactionWithStatus(TransactionDto transactionDto, String status) {
        Transaction transaction = Transaction.builder()
                .accountId(transactionDto.getAccountId())
                .amount(transactionDto.getAmount())
                .dateTime(LocalDateTime.now())
                .transactionStatus(status)
                .build();

        System.out.println("Transaction saved: " + transaction.getAccountId() + " " + transaction.getAmount() + " " + transaction.getDateTime() + " " + transaction.getTransactionStatus());
        return transactionRepository.save(transaction);
    }

    @Override
    public void updateTransactionStatus(Long id, String status) {
        Transaction transaction = transactionRepository.findById(id).orElse(null);
        if (transaction != null) {
            transaction.setTransactionStatus(status);
        }
    }

    @Override
    public Long userTransactionsCountInInterval(Long userId, LocalDateTime dateTime) {
        Collection<Transaction> transactions = transactionRepository.findByAccountIdAndDateTimeGreaterThan(userId, dateTime);
        Long transactionsCount = (long) transactions.size();
        return transactionsCount;
    }
}
