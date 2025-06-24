package com.example.spring_boot;


import com.example.spring_boot.listener.JobCompletionListener;
import com.example.spring_boot.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import org.springframework.batch.item.database.JdbcBatchItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.database.builder.JdbcBatchItemWriterBuilder;
import org.springframework.batch.item.database.builder.JdbcCursorItemReaderBuilder;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;

import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;
import java.util.Arrays;

@Configuration
public class BatchConfig {

    @Bean
    public Job simpleJob(JobRepository jobRepository,Step dbStep1, Step dbStep2, DataSource dataSource ) {
        return new JobBuilder("simpleJob", jobRepository)
                .start(dbStep1)
                .next(dbStep2)
                .listener(new JobCompletionListener(dataSource))
                .build();
    }




    //data db step
    @Bean
    public Step dbStep1(JobRepository jobRepository,
                         DataSourceTransactionManager transactionManager,DataSource dataSource) {
        return new StepBuilder("dbStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
                .reader(personJSONReader()) //read from json
                .writer(databaseWriter(dataSource)) // write to persons
                .build();
    }
    @Bean
    public Step dbStep2(JobRepository jobRepository,
                        DataSourceTransactionManager transactionManager,DataSource dataSource) {
        return new StepBuilder("dbStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
                .reader(databaseReader(dataSource)) //read from persons
                .processor(dbItemProcessor()) //toUpperCase
                .writer(databaseWriter2(dataSource)) // write to persons_backup
                .build();
    }

    //data json step
    @Bean
    public Step jsonStep(JobRepository jobRepository,
                        DataSourceTransactionManager transactionManager) {
        return new StepBuilder("jsonStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
                .reader(personJSONReader())
                .processor(jsonItemProcessor())
                .writer(jsonItemWriter())
                .build();
    }

    //data csv Step
    @Bean
    public Step csvStep(JobRepository jobRepository,
                        DataSourceTransactionManager transactionManager) {
        return new StepBuilder("csvStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
               // .reader(csvItemReader())
                .reader(jsonItemReader())
                .processor(jsonItemProcessor())
                .writer(jsonItemWriter())
                .build();
    }

    //csv methods
    @Bean
    public static FlatFileItemReader<Person> csvReader() {
        return new FlatFileItemReaderBuilder<Person>()
                .name("csvReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names("vorname", "nachname")
                .targetType(Person.class)
                .build();
    }

    @Bean
    public FlatFileItemWriter<Person> csvWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("csvWriter")
                .resource(new FileSystemResource("src/main/resources/sample-data-output.csv"))
                .delimited()
                .delimiter(",")
                .names("lastName", "firstName")
                .build();
    }
    //Json methods
    @Bean
    public FlatFileItemWriter<Person> jsonWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("jsonWriter")
                .resource(new FileSystemResource("src/main/resources/sample-data-output.csv"))
                .delimited()
                .delimiter(",")
                .names("lastName", "firstName")
                .build();
    }

    private JsonItemReader<Person> personJSONReader() {
        return new JsonItemReaderBuilder<Person>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(Person.class))
                .resource(new ClassPathResource("persons.json"))
                .name("personJsonItemReader")
                .build();
    }





    //Step csv Chunk
    private ItemReader<Person> csvItemReader() {
    return  csvReader();
    }

    private ItemProcessor<Person, Person> csvItemProcessor() {
        return item -> {
            System.out.println("Processing: " + item);

            String vorname = item.getFirstName();
            String modifiedVorname = convertVowelsToUppercase(vorname);

            return new Person(modifiedVorname, item.getLastName());
        };
    }
    private ItemWriter<Person> csvItemWriter() {
        return csvWriter();
    }

    //Step json Chunk
    private ItemProcessor<Person, Person> jsonItemProcessor() {
        return item -> {
            System.out.println("Processing: " + item);

            String vorname = item.getFirstName();
            String modifiedVorname = convertVowelsToUppercase(vorname);

            return new Person(modifiedVorname, item.getLastName());
        };
    }

    private ItemWriter<Person> jsonItemWriter() {
        return jsonWriter();
    }

    private ItemReader<Person> jsonItemReader() {
        return personJSONReader();
    }

    //step DB chunk

    private ItemProcessor<Person, Person> dbItemProcessor() {
        return item -> {
            String firstName = item.getFirstName().toUpperCase();
            String lastName = item.getLastName().toUpperCase();
            return new Person(firstName, lastName);
        };
    }

    private ItemWriter<Person> dbItemWriter() {
        return jsonWriter();
    }

    private ItemReader<Person> dbItemReader() {
        return personJSONReader();
    }





    //Daten aus der Datenbank lesen
    @Bean
    public JdbcCursorItemReader<Person> databaseReader(DataSource dataSource) {
        return new JdbcCursorItemReaderBuilder<Person>()
                .dataSource(dataSource)
                .name("databaseReader")
                .sql("SELECT first_name, last_name FROM persons")
                .rowMapper(new BeanPropertyRowMapper<>(Person.class))
                .build();
    }
   // Daten in die Datenbank schreiben
    @Bean
    public JdbcBatchItemWriter<Person> databaseWriter(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("INSERT INTO persons (first_name, last_name) VALUES (:firstName, :lastName)")
                .beanMapped()
                .build();

    }
    @Bean
    public JdbcBatchItemWriter<Person> databaseWriter2(DataSource dataSource) {
        return new JdbcBatchItemWriterBuilder<Person>()
                .dataSource(dataSource)
                .sql("INSERT INTO persons_backup (first_name, last_name) VALUES (:firstName, :lastName)")
                .beanMapped()
                .build();

    }
        //Utils

    private String convertVowelsToUppercase(String input) {
        if (input == null) return null;
        StringBuilder result = new StringBuilder();
        for (char c : input.toCharArray()) {
            if ("aeiouäöü".indexOf(c) != -1) {
                result.append(Character.toUpperCase(c));
            } else {
                result.append(c);
            }
        }
        return result.toString();
    }


}