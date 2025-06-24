package com.example.spring_boot.listener;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.jdbc.core.JdbcTemplate;
import javax.sql.DataSource;

public class JobCompletionListener implements JobExecutionListener {

    private JdbcTemplate jdbcTemplate;

    public JobCompletionListener(DataSource dataSource) {
        this.jdbcTemplate = new JdbcTemplate(dataSource);
    }

    @Override
    public void beforeJob(JobExecution jobExecution) {
        System.out.println("Job startet!!");
    }

    @Override
    public void afterJob(JobExecution jobExecution) {

        if (jobExecution.getStatus().isUnsuccessful()) {
            System.out.println("Job failed, no further checks.");
            return;
        }


        Integer count = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM persons", Integer.class);

        if (count > 0) {
            System.out.println("Data successfully written to the persons table. Total records: " + count);
        } else {
            System.out.println("No data found in the persons table.");
        }


        Integer backupCount = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM persons_backup", Integer.class);

        if (backupCount > 0) {
            System.out.println("Data successfully written to the persons_backup table. Total records: " + backupCount);
        } else {
            System.out.println("No data found in the persons_backup table.");
        }

        System.out.println("Job finished!!");
    }
}
