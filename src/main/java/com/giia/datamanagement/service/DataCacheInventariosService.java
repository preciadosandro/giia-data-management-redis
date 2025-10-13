package com.giia.datamanagement.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giia.datamanagement.repository.InventarioRepository;
import com.giia.datamanagement.repository.MaterialRepository;
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
public class DataCacheInventariosService {

    private final InventarioRepository inventarioRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final String keyPrefix;
    private final SseService sseService;
    private final ObjectMapper objectMapper;


    public DataCacheInventariosService( InventarioRepository inventarioRepository,
                                    ReactiveRedisTemplate<String, String> redisTemplate,
                                    ReactiveRedisConnectionFactory connectionFactory,
                                    @Value("${datacache.redis.key-prefix.inventario}") String keyPrefix,
                                    @Value("${datacache.redis.channel.inventario}") String channel,
                                    SseService sseService, ObjectMapper objectMapper) {
        this.inventarioRepository = inventarioRepository;
        this.redisTemplate = redisTemplate;
        ReactiveRedisMessageListenerContainer listenerContainer = new ReactiveRedisMessageListenerContainer(connectionFactory);
        ChannelTopic channelTopic = new ChannelTopic(channel);
        this.keyPrefix = keyPrefix;
        this.sseService = sseService;
        this.objectMapper=objectMapper;


        listenerContainer.receive(channelTopic)
                .map(ReactiveSubscription.Message::getMessage)
                .cast(String.class)
                .flatMap(event ->{
                            log.debug("Evento leido provedor service: {}",event);
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
                        inventarioRepository.findAll()
                                .flatMap(inventario -> {
                                    String redisKey = keyPrefix + inventario.getId();
                                    try {
                                        String json = objectMapper.writeValueAsString(inventario);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return Mono.error(new RuntimeException("Error serializando inventario", e));
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    sseService.publish("REFRESH_INVENTARIO");
                    log.debug("Cache de inventario refrescada desde SQL Server");
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
