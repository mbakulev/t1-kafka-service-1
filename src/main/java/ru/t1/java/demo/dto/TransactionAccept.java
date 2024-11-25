package ru.t1.java.demo.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class TransactionAccept {
    private Long transactionId;
    private Long userId;
    private Long accountId;
    private LocalDateTime dateTime;
    private BigDecimal transactionAmount;
    private BigDecimal accountBalance;
}