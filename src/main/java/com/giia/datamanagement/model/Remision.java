package com.giia.datamanagement.model;

import lombok.Getter;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.MappedCollection;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;
import java.util.List;

@Getter
@Setter
@Table("Remisiones")
public class Remision {
    @Id
    private Long id;

    @Column("numero_remision")
    private String numeroRemision;

    @Column("proveedor_id")
    private Long proveedorId;

    @Column("fecha_creacion")
    private LocalDateTime fechaCreacion;

    @Column("estado_id")
    private Long estadoId;

    @Column("Observaciones")
    private String observaciones;




}
