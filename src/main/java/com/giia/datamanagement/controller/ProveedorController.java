package com.giia.datamanagement.controller;

import com.giia.datamanagement.service.*;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequiredArgsConstructor
@RequestMapping("/refresh")
public class ProveedorController {

    private final DataCacheProvedoresService dataCacheService;

    private final DataCacheMaterialService dataCacheMaterialService;
    private final DataCacheRemisionesService dataCacheRemisionesService;
    private final DataCacheInventariosService dataCacheInventariosService;
    private final DataCacheUsuarioService dataCacheUsuarioService;

    /**
     * Refrescar cache desde BD - Proveedores
     */
    @GetMapping("/proveedores")
    public Mono<ResponseEntity<String>> generateRefreshProveedores() {
        return dataCacheService.refreshAll()
                .then(Mono.just(ResponseEntity.ok("Cache de proveedores refrescado exitosamente")));
    }

    /**
     * Refrescar cache desde BD - Usuarios
     */
    @GetMapping("/usuarios")
    public Mono<ResponseEntity<String>> generateRefreshUsuarios() {
        return dataCacheUsuarioService.refreshAllUsu()
                .then(Mono.just(ResponseEntity.ok("Cache de usuarios refrescado exitosamente")));
    }

    /**
     * Refrescar cache desde BD - Materiales
     */
    @GetMapping("/materiales")
    public Mono<ResponseEntity<String>> generateRefreshMateriales() {
        return dataCacheMaterialService.refreshAll()
                .then(Mono.just(ResponseEntity.ok("Cache de materiales refrescado exitosamente")));
    }

    /**
     * Refrescar cache desde BD - Remisiones
     */
    @GetMapping("/remisiones")
    public Mono<ResponseEntity<String>> generateRefreshRemisiones() {
        return dataCacheRemisionesService.refreshAll()
                .then(Mono.just(ResponseEntity.ok("Cache de remisiones refrescado exitosamente")));
    }

    /**
     * Refrescar cache desde BD - Inventarios
     */
    @GetMapping("/inventarios")
    public Mono<ResponseEntity<String>> generateRefreshInventarios() {
        return dataCacheInventariosService.refreshAll()
                .then(Mono.just(ResponseEntity.ok("Cache de inventarios refrescado exitosamente")));
    }




}
