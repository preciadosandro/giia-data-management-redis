package com.giia.datamanagement.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giia.datamanagement.repository.ProveedorRepository;
import com.giia.datamanagement.repository.UsuarioProveedorRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.connection.ReactiveSubscription;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class DataCacheUsuarioService {

    private final UsuarioProveedorRepository usuarioProveedorRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;

    private final String keyPrefixUsu;
    private final SseService sseService;
    private final ObjectMapper objectMapper;

    public DataCacheUsuarioService(UsuarioProveedorRepository usuarioProveedorRepository,
                                   ReactiveRedisTemplate<String, String> redisTemplate,
                                   ReactiveRedisConnectionFactory connectionFactory,
                                   @Value("${datacache.redis.key-prefix.usuario-proveedor}") String keyPrefixUsu,
                                   @Value("${datacache.redis.channel.usuario-proveedor}") String channel,
                                   SseService sseService, ObjectMapper objectMapper) {
        this.redisTemplate = redisTemplate;
        this.keyPrefixUsu = keyPrefixUsu;
        this.sseService = sseService;
        this.usuarioProveedorRepository =usuarioProveedorRepository;
        this.objectMapper=objectMapper;
        ReactiveRedisMessageListenerContainer listenerContainer = new ReactiveRedisMessageListenerContainer(connectionFactory);
        ChannelTopic channelTopic = new ChannelTopic(channel);

        listenerContainer.receive(channelTopic)
                .map(ReactiveSubscription.Message::getMessage)
                .cast(String.class)
                .flatMap(event ->{
                            log.debug("Evento leido provedor service: {}",event);
                            return refreshAllUsu();
                        }
                )
                .onErrorResume(e -> {
                    log.error("Error parseando mensaje Redis", e);
                    return Mono.empty();
                })
                .subscribe();
    }

    @PostConstruct
    public void init() {
        redisTemplate.delete("*");
        refreshAllUsu().subscribe();
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

    public Mono<Void> refreshAllUsu() {
        return redisTemplate.keys(keyPrefixUsu + "*") // 1. Trae todas las keys de proveedores
                .flatMap(redisTemplate::delete)     // 2. Borra cada key
                .thenMany(                          // 3. Una vez borrado, vuelve a insertar
                        usuarioProveedorRepository.findAll()
                                .flatMap(usuario -> {
                                    String redisKey = keyPrefixUsu + usuario.getId();
                                    try {
                                        String json = objectMapper.writeValueAsString(usuario);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return Mono.error(new RuntimeException("Error serializando usuario", e));
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    refreshAll();
                    sseService.publish("REFRESH_USUARIOS");
                    log.debug("Cache de usuarios refrescada desde SQL Server");
                });
    }
    /**
     * Polling de respaldo: cada hora refresca todo
     */
    @Scheduled(fixedRate = 3600000) // 1 hora en ms
    public void scheduledRefresh() {
        refreshAllUsu().subscribe();
    }




}
