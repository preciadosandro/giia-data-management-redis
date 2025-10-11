package com.giia.datamanagement.model;


import lombok.*;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;


@Getter
@Setter
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Table("Proveedores")
public class Proveedor {
    @Id
    private Long id;
    @Column("nit")
    private String nit;
    @Column("nombre")
    private String nombre;
    @Column("email")
    private String email;
    @Column("telefono")
    private String telefono;
    @Column("direccion")
    private String direccion;
    @Column("activo")
    private String activo;

    private Long idUsuario;
}