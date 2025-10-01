package com.giia.datamanagement.model;

import jakarta.persistence.Entity;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
@Table(name = "Usuario")
public class Usuario {

    @Id
    private Integer id;

    @Column("usuario")
    private String usuario;

    @Column("nombre")
    private String nombre;

    @Column("email")
    private String email;

    @Column("password_hash")
    private String passwordHash;

    @Column("rol_id")
    private Integer rolId;

    @Column("fecha_creacion")
    private LocalDateTime fechaCreacion;

    @Column("activo")
    private Boolean activo;

}