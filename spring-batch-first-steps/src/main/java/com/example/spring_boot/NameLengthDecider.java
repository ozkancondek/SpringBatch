package com.example.spring_boot;

import com.example.spring_boot.model.Person;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.stereotype.Component;

@Component
public class NameLengthDecider implements JobExecutionDecider {

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {

        Person person = (Person) stepExecution.getExecutionContext().get("person");


        if (person != null && (person.getFirstName().length() > 10 || person.getLastName().length() > 10)) {

            System.out.println("Person " + person.getFirstName() + " " + person.getLastName() + " has a long name, skipped.");
            return new FlowExecutionStatus("SKIPPED");
        }

        return new FlowExecutionStatus("COMPLETED");
    }
}
