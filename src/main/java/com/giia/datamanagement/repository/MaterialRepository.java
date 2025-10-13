package com.giia.datamanagement.repository;

import com.giia.datamanagement.model.Material;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;


public interface MaterialRepository extends ReactiveCrudRepository<Material, Long> {}
