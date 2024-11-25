package ru.t1.java.demo.model;

import jakarta.persistence.*;
import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.jpa.domain.AbstractPersistable;

@Getter
@Setter
@Entity
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "data_source_error_log")
public class DataSourceErrorLog  {
    @jakarta.persistence.Id
    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "data_source_error_log_sequense")
    @SequenceGenerator(name = "data_source_error_log_sequense", sequenceName = "data_source_error_log_sequense", allocationSize = 1)
    private Long id;

    @Column(name = "message")
    private String message;

    @Column(name = "stacktrace")
    private String stackTrace;

    @Column(name = "method_name")
    private String methodName;
}
