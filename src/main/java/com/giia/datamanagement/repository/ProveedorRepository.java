package com.giia.datamanagement.repository;

import com.giia.datamanagement.model.Proveedor;
import org.springframework.data.repository.reactive.ReactiveCrudRepository;


public interface ProveedorRepository extends ReactiveCrudRepository<Proveedor, Long> {
}
