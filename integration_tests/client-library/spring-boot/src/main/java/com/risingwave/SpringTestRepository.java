package com.risingwave;

import java.util.List;
import org.springframework.data.repository.CrudRepository;
public interface SpringTestRepository extends CrudRepository<SpringTest, Integer>{
    List<SpringTest> findByName(String name);
    SpringTest findById(int id);
}
