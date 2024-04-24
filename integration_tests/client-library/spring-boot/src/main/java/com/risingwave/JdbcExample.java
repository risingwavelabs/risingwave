package com.risingwave;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.core.JdbcTemplate;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Objects;

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
            SpringTest bob = new SpringTest(2, true, (short) 10, 100, 1000, (float) 1.1, -9.99, "Bob", OffsetDateTime.of(2024, 1, 1, 10, 0, 59, 0, ZoneOffset.UTC));
            repository.save(alice);
            repository.save(bob);
            flush();

            SpringTest alice_tmp = repository.findById(alice.getId()).get();
            check(alice, alice_tmp);

            SpringTest bob_tmp = repository.findByName(bob.getName()).get(0);
            check(bob, bob_tmp);

            // update
            alice.setInteger(-1);
            alice.setDouble(9999.9999);
            repository.save(alice);
            flush();
            alice_tmp = repository.findById(alice.getId()).get();
            check(alice, alice_tmp);

            // delete
            repository.deleteById(alice.getId());
            assert repository.findById(alice.getId()).isEmpty();
            dropTable();
            System.out.println("done");
        };
    }

    private void check(SpringTest origin, SpringTest target) {
        assert origin.getId() == target.getId();
        assert Objects.equals(origin.getName(), target.getName());
        assert origin.getBoolean() == target.getBoolean();
        assert origin.getSmallint() == target.getSmallint();
        assert origin.getInteger() == target.getInteger();
        assert origin.getBigint() == target.getBigint();
        assert origin.getFloat() == target.getFloat();
        assert origin.getDouble() == target.getDouble();
        assert origin.getTimestampTZ().equals(target.getTimestampTZ());
    }

    private void flush() {
        String sql = "FLUSH;";
        jdbcTemplate.execute(sql);
    }

    private void dropTable() {
        String sql = "DROP TABLE IF EXISTS springtest;";
        System.out.println("running query: "+sql);
        jdbcTemplate.execute(sql);
        System.out.println("drop table done");
    }

    private void createTable() {
        String sql = """
        CREATE TABLE springtest (
          id INT,
          cboolean boolean,
          csmallint smallint,
          cinteger integer,
          cbigint bigint,
          cfloat real,
          cdouble double precision,
          name varchar,
          ctimestamptz timestamptz,
          PRIMARY KEY (id)
          );
        """;
        System.out.println("running query: "+sql);
        jdbcTemplate.execute(sql);
        System.out.println("create table done");
    }
}
