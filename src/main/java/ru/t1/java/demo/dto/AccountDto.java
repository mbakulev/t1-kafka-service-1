package ru.t1.java.demo.dto;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.Column;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import ru.t1.java.demo.dto.enums.AccountStatus;
import ru.t1.java.demo.model.AccountType;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * DTO for {@link ru.t1.java.demo.model.Account}
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
@JsonIgnoreProperties(ignoreUnknown = true)
public class AccountDto implements Serializable {
    private Long id;

    @JsonProperty("client_id")
    private Long clientId;

    @JsonProperty("account_type")
    private AccountType accountType;

    @JsonProperty("account_balance")
    private BigDecimal balance;

    @JsonProperty("account_status")
    private String accountStatus;

    @JsonProperty("frozen_ammount")
    private BigDecimal frozenAmount;
}