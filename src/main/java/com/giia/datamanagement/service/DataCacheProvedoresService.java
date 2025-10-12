package com.giia.datamanagement.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giia.datamanagement.repository.ProveedorRepository;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory;
import org.springframework.data.redis.listener.ChannelTopic;
import org.springframework.data.redis.listener.ReactiveRedisMessageListenerContainer;
import org.springframework.data.redis.connection.ReactiveSubscription.Message;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@Slf4j
public class DataCacheProvedoresService {

    private final ProveedorRepository proveedorRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final String keyPrefix;
    private final SseService sseService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final DataCacheUsuarioService usuarioService;

    public DataCacheProvedoresService(ProveedorRepository proveedorRepository,
                                      ReactiveRedisTemplate<String, String> redisTemplate,
                                      ReactiveRedisConnectionFactory connectionFactory,
                                      @Value("${datacache.redis.key-prefix.proveedor}") String keyPrefix,
                                      @Value("${datacache.redis.channel.proveedor}") String channel,
                                      SseService sseService, DataCacheUsuarioService usuarioService) {
        this.proveedorRepository = proveedorRepository;
        this.redisTemplate = redisTemplate;
        ReactiveRedisMessageListenerContainer listenerContainer = new ReactiveRedisMessageListenerContainer(connectionFactory);
        ChannelTopic channelTopic = new ChannelTopic(channel);
        this.keyPrefix = keyPrefix;
        this.sseService = sseService;
        this.usuarioService=usuarioService;

        listenerContainer.receive(channelTopic)
                .map(Message::getMessage)
                .cast(String.class)
                .flatMap(event ->{
                            log.debug("Evento leido provedor service: {}",event);
                            usuarioService.refreshAllUsu();
                            return refreshAll();
                        }
                )
                .onErrorResume(e -> {
                    log.error("Error parseando mensaje Redis", e);
                    return Mono.empty();
                })
                .subscribe();
    }

    /**
     * Refresca toda la tabla desde SQL Server y la reescribe en Redis
     */
    @PostConstruct
    public void init() {
        refreshAll().subscribe();
    }

    public Mono<Void> refreshAll() {
        return redisTemplate.keys(keyPrefix + "*") // 1. Trae todas las keys de proveedores
                .flatMap(redisTemplate::delete)     // 2. Borra cada key
                .thenMany(                          // 3. Una vez borrado, vuelve a insertar
                        proveedorRepository.findAll()
                                .flatMap(proveedor -> {
                                    String redisKey = keyPrefix + proveedor.getId();
                                    try {
                                        String json = objectMapper.writeValueAsString(proveedor);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return Mono.error(new RuntimeException("Error serializando proveedor", e));
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    sseService.publish("REFRESH_PROVEEDORES");
                    log.debug("Cache de proveedores refrescada desde SQL Server");
                });
    }
    /**
     * Polling de respaldo: cada hora refresca todo
     */
    @Scheduled(fixedRate = 3600000) // 1 hora en ms
    public void scheduledRefresh() {
        refreshAll().subscribe();
    }


}
