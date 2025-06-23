package com.example.spring_boot;


import com.example.spring_boot.model.Person;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;

import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.batch.item.file.builder.FlatFileItemWriterBuilder;

import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.Arrays;

@Configuration
public class BatchConfig {

    @Bean
    public Job simpleJob(JobRepository jobRepository,Step csvStep) {
        return new JobBuilder("simpleJob", jobRepository)
                .start(csvStep)
//                .next(csvStep)
                .build();
    }

    //read csv Step
    @Bean
    public Step csvStep(JobRepository jobRepository,
                        DataSourceTransactionManager transactionManager) {
        return new StepBuilder("csvStep", jobRepository)
                .<Person, Person>chunk(5, transactionManager)
                .reader(csvItemReader())
                .processor(csvItemProcessor())
                .writer(csvItemWriter())
                .build();
    }

    @Bean
    public FlatFileItemWriter<Person> csvWriter() {
        return new FlatFileItemWriterBuilder<Person>()
                .name("csvWriter")
                .resource(new FileSystemResource("src/main/resources/sample-data-output.csv"))
                .delimited()
                .delimiter(",")
                .names("nachname", "vorname")
                .build();
    }

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

    //Step csv Chunk
    private ItemReader<Person> csvItemReader() {
    return  csvReader();
    }

    private ItemProcessor<Person, Person> csvItemProcessor() {
        return item -> {
            System.out.println("Processing: " + item);

            String vorname = item.vorname();
            String modifiedVorname = convertVowelsToUppercase(vorname);

            return new Person(modifiedVorname, item.nachname());
        };
    }

    private ItemWriter<Person> csvItemWriter() {
        return csvWriter();
    }


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