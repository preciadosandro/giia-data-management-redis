package com.giia.datamanagement.repository;

import com.giia.datamanagement.model.Remision;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
public interface RemisionRepository extends ReactiveCrudRepository<Remision, Long> {
}
