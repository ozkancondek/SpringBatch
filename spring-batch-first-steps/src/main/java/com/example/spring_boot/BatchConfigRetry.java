package com.example.spring_boot;

import com.example.spring_boot.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
public class BatchConfigRetry {
    @Bean
    public Job retryJob(JobRepository jobRepository,
                         Step retryStep ) {
        return new JobBuilder("retryJob", jobRepository)
                .start(retryStep)
                .build();
    }

    //Simith exception
    public class SmithException extends RuntimeException {
        public SmithException(String message) {
            super(message);
        }
    }

    //retryProcessor
    @Bean
    public ItemProcessor<Person, Person> retryProcessor() {
        return item -> {
            if (item.getLastName().equals("Smith")) {
                throw new SmithException("Name with Smith found! process fail");
            }
            return item;
        };
    }

    //retryWriter
    @Bean
    public ItemWriter<Person> retryWriter() {
        return items -> {
            for (Person person : items) {
                System.out.println("person: " + person.getFirstName() + " " + person.getLastName());
            }
        };
    }

    //retryStep
    @Bean
    public Step retryStep(JobRepository jobRepository, DataSourceTransactionManager transactionManager) {
        return new StepBuilder("retryStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(personJSONReader())
                .processor(retryProcessor())
                .writer(retryWriter())   //write to console
                .faultTolerant()
                .retry( SmithException.class)  // try again with simith exception
                .retryLimit(3)  // max 3 retry
                .skip( SmithException.class)
                .skipLimit(3)  // max 3 skip
                .build();
    }

    private JsonItemReader<Person> personJSONReader() {
        return new JsonItemReaderBuilder<Person>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Person.class))
                .resource(new ClassPathResource("persons.json"))
                .name("personJsonItemReader")
                .build();
    }
}


