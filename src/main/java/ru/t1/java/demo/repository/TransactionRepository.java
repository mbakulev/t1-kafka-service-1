package ru.t1.java.demo.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import ru.t1.java.demo.model.Transaction;

import java.time.LocalDateTime;
import java.util.Collection;
import java.util.Optional;

public interface TransactionRepository extends JpaRepository<Transaction, Long> {
    @Override
    Optional<Transaction> findById(Long aLong);
    Collection<Transaction> findByAccountIdAndDateTimeGreaterThan(Long userId, LocalDateTime localDateTime);
}
