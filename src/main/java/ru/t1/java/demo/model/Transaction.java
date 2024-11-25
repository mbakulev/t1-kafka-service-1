package ru.t1.java.demo.model;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.jpa.domain.AbstractPersistable;
import ru.t1.java.demo.dto.enums.TransactionStatus;

import java.math.BigDecimal;
import java.time.LocalDateTime;

@Getter
@Setter
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "transaction")
public class Transaction {
    @jakarta.persistence.Id
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "transaction_sequense")
    @SequenceGenerator(name = "transaction_sequense", sequenceName = "transaction_sequense", allocationSize = 1)
    private Long id;

//    @Column(name = "id")
//    private Long id;

    @Column(name = "account_id")
    private Long accountId;

    @Column(name = "amount")
    private BigDecimal amount;

    @Column(name = "timestamp")
    private LocalDateTime dateTime;

    @Column(name = "status")
    private String transactionStatus;
}
