package com.example.spring_boot.reader;

import com.example.spring_boot.model.Person;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.builder.FlatFileItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;

import java.util.Iterator;
import java.util.List;

public class CsvReader implements ItemReader<Person> {

    private final FlatFileItemReader<Person> delegate;
    private Iterator<Person> personIterator;

    // Constructor ile dosyayı okuma başlatıyoruz
    public CsvReader() {
        delegate = new FlatFileItemReaderBuilder<Person>()
                .name("csvReader")
                .resource(new ClassPathResource("sample-data.csv"))
                .delimited()
                .names("vorname", "nachname")
                .targetType(Person.class)
                .build();
    }

    @Override
    public Person read() throws Exception {
        if (personIterator == null) {
            // İlk kez okuma başladığında tüm veriyi okur ve bir iterator oluşturur
            List<Person> personList = readFile();
            personIterator = personList.iterator();
        }

        if (personIterator.hasNext()) {
            Person person = personIterator.next();
            System.out.println("Reading: " + person);  // Okunan itemi ekrana yazdır
            return person;
        }

        return null;  // Veriler tükenmişse null döndür
    }

    private List<Person> readFile() throws Exception {
        delegate.open(delegate.getExecutionContext());
        List<Person> personList = delegate.read();
        delegate.close();
        return personList;
    }
}
