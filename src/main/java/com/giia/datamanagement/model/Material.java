// Material.java
package com.giia.datamanagement.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "Materiales")
public class Material {
    @Id
    private Long id;
    private String nombre;
    private String descripcion;
    private String unidadMedida;
    private Long categoriaId;
    private String barcode;
    private LocalDateTime fechaCreacion;
    private Boolean activo;


}
