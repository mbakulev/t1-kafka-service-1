package ru.t1.java.demo.model;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.jpa.domain.AbstractPersistable;
import ru.t1.java.demo.dto.enums.AccountStatus;

import java.math.BigDecimal;

@Getter
@Setter
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "account")
public class Account {
    @jakarta.persistence.Id
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "account_sequense")
    @SequenceGenerator(name = "account_sequense", sequenceName = "account_sequense", allocationSize = 1)
    private Long id;

    @Column(name = "client_id")
    private Long clientId;

    @Column(name = "account_type")
    private String type;

    @Column(name = "balance")
    private BigDecimal balance;

    @Column(name = "status")
    private String accountStatus;

    @Column(name = "frozen_amount")
    private BigDecimal frozenAmount;
}