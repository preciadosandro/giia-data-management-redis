package com.giia.datamanagement.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giia.datamanagement.repository.ProveedorRepository;
import com.giia.datamanagement.repository.UsuarioProveedorRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DataCacheUsuarioService {

    private final ProveedorRepository proveedorRepository;
    private final UsuarioProveedorRepository usuarioProveedorRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    private final String keyPrefixUsu;
    private final SseService sseService;
    private final ObjectMapper objectMapper;

    public DataCacheUsuarioService(ProveedorRepository proveedorRepository,UsuarioProveedorRepository usuarioProveedorRepository,
                                   ReactiveRedisTemplate<String, String> redisTemplate,
                                   @Value("${datacache.redis.key-prefix.usuario-proveedor}") String keyPrefixUsu,
                                   SseService sseService, ObjectMapper objectMapper) {
        this.proveedorRepository = proveedorRepository;
        this.redisTemplate = redisTemplate;
        this.keyPrefixUsu = keyPrefixUsu;
        this.sseService = sseService;
        this.usuarioProveedorRepository =usuarioProveedorRepository;
        this.objectMapper=objectMapper;

    }

    @PostConstruct
    public void init() {
        redisTemplate.delete("*");
        refreshAll();
    }

    /**
     * Refresca toda la tabla desde SQL Server y la reescribe en Redis
     */
    public void refreshAll() {
        redisTemplate.keys(keyPrefixUsu + "*")       // 1. Buscar todas las keys de usuario-proveedor
                .flatMap(redisTemplate::delete)      // 2. Borrarlas
                .thenMany(                           // 3. Insertar desde la BD
                        usuarioProveedorRepository.findAll()
                                .flatMap(usuario -> {
                                    String redisKey = keyPrefixUsu + usuario.getUsuario();
                                    try {
                                        String json = objectMapper.writeValueAsString(usuario);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return reactor.core.publisher.Mono.error(
                                                new RuntimeException("Error deserializando Usuario", e)
                                        );
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    sseService.publish("REFRESH_LOGIN");
                    log.debug("Cache de login de administradores refrescada");
                })
                .subscribe();
    }



}
