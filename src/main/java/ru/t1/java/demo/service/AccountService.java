package ru.t1.java.demo.service;

import ru.t1.java.demo.dto.AccountDto;
import ru.t1.java.demo.model.Account;

import java.math.BigDecimal;
import java.util.Optional;

public interface AccountService {
    Account getAccount(Long id);
    Account createAccount(AccountDto dto);
    Account updateAccount(AccountDto account);
    void deleteAccount(Long id);
    void saveAccount(AccountDto account);
    String checkAccountStatus(Long id);
    Account changeAccountBalance(Long id, BigDecimal balance);
    void setFrozenAmount(Long id, BigDecimal amount);
    Account blockAccount(Long id);
}
