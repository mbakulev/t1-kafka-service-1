package ru.t1.java.demo.service.impl;

import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import ru.t1.java.demo.dto.AccountDto;
import ru.t1.java.demo.model.Account;
import ru.t1.java.demo.model.Transaction;
import ru.t1.java.demo.repository.AccountRepository;
import ru.t1.java.demo.service.AccountService;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.Optional;

@Service
@RequiredArgsConstructor
public class AccountServiceImpl implements AccountService {

    private final AccountRepository accountRepository;

    @Override
    public Account getAccount(Long id) {
        return accountRepository.findById(id).orElse(null);
    }

    @Override
    public Account createAccount(AccountDto accountDto) {
        Account account = Account.builder()
                .clientId(accountDto.getClientId())
                .type(String.valueOf(accountDto.getAccountType()))
                .balance(accountDto.getBalance())
                .build();;

        System.out.println("new account: " + account);
        return accountRepository.save(account);
    }

    @Override
    public Account updateAccount(AccountDto accountDto) {
        Account account = getAccount(accountDto.getId());
        account.setClientId(accountDto.getClientId());
        account.setBalance(accountDto.getBalance());

        return accountRepository.save(account);
    }

    @Override
    public void deleteAccount(Long id) {
        accountRepository.deleteById(id);
    }

    @Override
    public void saveAccount(AccountDto accountDto) {
        Account account = Account.builder()
                .type(accountDto.getAccountType().toString())
                .clientId(accountDto.getClientId())
                .balance(accountDto.getBalance())
                .build();

        System.out.println("save account: " + account.toString());
        accountRepository.save(account);
    }

    @Override
    public String checkAccountStatus(Long id) {
        Optional<Account> account = accountRepository.findById(id);
        if (account.isPresent()) {return account.get().getAccountStatus().toString();}
        return null;
    }

    @Override
    public Account changeAccountBalance(Long id, BigDecimal transactionAmount) {
        Account account = getAccount(id);
        if (account != null) {
            BigDecimal oldBalance = account.getBalance();
            BigDecimal newBalance = oldBalance.add(transactionAmount);
            account.setBalance(newBalance);

            return accountRepository.save(account);
        }

        return null;
    }

    @Override
    public void setFrozenAmount(Long id, BigDecimal amount) {
        Account account = getAccount(id);
        if (account != null) {
            BigDecimal oldFrozenAmount = account.getFrozenAmount();
            BigDecimal newFrozenAmount = oldFrozenAmount.add(amount);
            account.setFrozenAmount(newFrozenAmount);

            accountRepository.save(account);
        }
    }
}
