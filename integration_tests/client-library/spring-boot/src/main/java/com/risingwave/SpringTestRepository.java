package com.risingwave;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.CrudRepository;
public interface SpringTestRepository extends CrudRepository<SpringTest, Integer>{
    List<SpringTest> findByName(String name);
    Optional<SpringTest> findById(int id);
}
