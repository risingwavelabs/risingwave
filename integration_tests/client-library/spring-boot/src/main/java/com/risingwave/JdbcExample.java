package com.risingwave;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.util.List;

@SpringBootApplication
public class JdbcExample {
    @Autowired
    private JdbcTemplate jdbcTemplate;

    public static void main(String[] args) {
        SpringApplication.run(JdbcExample.class, args);
    }

    @Bean
    public CommandLineRunner commandLineRunner(SpringTestRepository repository) {
        return args -> {
            createTable();
            SpringTest alice = new SpringTest(1, "Alice");
            SpringTest bob = new SpringTest(2, "Bob");
            repository.save(alice);
            repository.save(bob);
            repository.save(new SpringTest(3, "Bob"));

            SpringTest tmp = repository.findById(1);
            assert tmp.getName().equals("Alice");

            List<SpringTest> tmp2 = repository.findByName("Bob");
            for (SpringTest st : tmp2) {
                System.out.println(st.getId()+st.getName());
            }
            System.out.println(tmp2.size());

            System.out.println("done");
        };
    }

    private void createTable() {
        String sql = "CREATE TABLE springtest (id INT, name VARCHAR);";
        System.out.println("running query: "+sql);
        jdbcTemplate.execute(sql);
        System.out.println("create table done");
    }
}
