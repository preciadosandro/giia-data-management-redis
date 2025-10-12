package com.giia.datamanagement.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.springframework.data.annotation.Id;
import org.springframework.data.relational.core.mapping.Column;
import org.springframework.data.relational.core.mapping.Table;

import java.math.BigDecimal;

@Getter
@Setter
@Table("Remisiones_Materiales")
@NoArgsConstructor
public class RemisionMaterial {

    public RemisionMaterial(Long remisionId, Long materialId, BigDecimal cantidad){
     this.remisionId=remisionId;
     this.materialId=materialId;
     this.cantidad=cantidad;
    }
    @Id
    private Long id;

    @Column("remision_id")
    private Long remisionId;

    @Column("material_id")
    private Long materialId;

    @Column("cantidad")
    private BigDecimal cantidad;


	
}
