package com.giia.datamanagement.repository;

import com.giia.datamanagement.model.Inventario;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;
import org.springframework.stereotype.Repository;



public interface InventarioRepository extends ReactiveCrudRepository<Inventario, Long> {

}
