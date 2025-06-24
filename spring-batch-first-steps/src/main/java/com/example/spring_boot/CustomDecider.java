package com.example.spring_boot;

import com.example.spring_boot.model.Person;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.Job;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;

@Configuration
public class CustomDecider {
    @Bean
    public Job deciderJob(JobRepository jobRepository, Step step1,
                          @Qualifier("successStep") Step successStep,
                          @Qualifier("failStep") Step failStep) {
        return new JobBuilder("deciderJob", jobRepository)
                .start(step1)
                .on("COMPLETED")
                .to(successStep)
                .from(step1)
                .on("SKIPPED")
                .to(failStep)
                .end()
                .build();
    }

    @Bean
    public Step step1(JobRepository jobRepository, DataSourceTransactionManager transactionManager, NameLengthDecider decider) {
        return new StepBuilder("step1", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(personJSONReader())
                .processor(nameLengthProcessor())
                .writer(personWriter())
                .listener(decider)
                .build();
    }

    private JsonItemReader<Person> personJSONReader() {
        return new JsonItemReaderBuilder<Person>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Person.class))
                .resource(new ClassPathResource("persons.json"))
                .name("personJsonItemReader")
                .build();
    }

    @Bean
    public ItemProcessor<Person, Person> nameLengthProcessor() {
        return item -> item;  // Bu adımda işlem yapmıyoruz çünkü decider zaten veriyi atıyor
    }

    @Bean
    public ItemWriter<Person> personWriter() {
        return items -> {
            for (Person person : items) {
                System.out.println("Processed: " + person.getFirstName() + " " + person.getLastName());
            }
        };
    }


    @Bean
    public Step successStep(JobRepository jobRepository, DataSourceTransactionManager transactionManager) {
        return new StepBuilder("successStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(personJSONReader())
                .writer(successWriter())
                .build();
    }

    @Bean
    public ItemWriter<Person> successWriter() {
        return items -> {
            for (Person person : items) {
                System.out.println("Successfully processed: " + person.getFirstName() + " " + person.getLastName());
            }
        };
    }

    @Bean
    public Step failStep(JobRepository jobRepository, DataSourceTransactionManager transactionManager) {
        return new StepBuilder("failStep", jobRepository)
                .<Person, Person>chunk(10, transactionManager)
                .reader(personJSONReader())
                .writer(failWriter())
                .build();
    }

    @Bean
    public ItemWriter<Person> failWriter() {
        return items -> {
            for (Person person : items) {
                System.out.println("Skipped/Failed processing: " + person.getFirstName() + " " + person.getLastName());
            }
        };
    }

}
