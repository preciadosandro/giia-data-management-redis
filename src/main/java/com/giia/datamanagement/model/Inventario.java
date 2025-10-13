// Inventario.java
package com.giia.datamanagement.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;


@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "inventario")
public class Inventario {
    @Id
    private Long id;
    private Long materialId;
    private BigDecimal cantidadActual;
    private BigDecimal stockMinimo;
    private String ubicacion;

}
