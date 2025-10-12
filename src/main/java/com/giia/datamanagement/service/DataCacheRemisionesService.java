package com.giia.datamanagement.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giia.datamanagement.repository.ProveedorRepository;
import com.giia.datamanagement.repository.RemisionMaterialRepository;
import com.giia.datamanagement.repository.RemisionRepository;
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
public class DataCacheRemisionesService {
    private final RemisionRepository remisionRepository;
    private final RemisionMaterialRepository remisionMaterialRepository;
    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final String keyPrefix;
    private final String keyPrefixMaterial;
    private final SseService sseService;
    private final ObjectMapper objectMapper;

    public DataCacheRemisionesService(RemisionRepository remisionRepository,
                                      RemisionMaterialRepository remisionMaterialRepository,
                                      ReactiveRedisTemplate<String, String> redisTemplate,
                                      ReactiveRedisConnectionFactory connectionFactory,
                                      @Value("${datacache.redis.key-prefix.remision}") String keyPrefix,
                                      @Value("${datacache.redis.key-prefix.remision-material}") String keyPrefixMaterial,
                                      @Value("${datacache.redis.channel.remision}") String channel,
                                      SseService sseService,
                                      ObjectMapper objectMapper) {
        this.remisionRepository = remisionRepository;
        this.remisionMaterialRepository=remisionMaterialRepository;
        this.redisTemplate = redisTemplate;
        ReactiveRedisMessageListenerContainer listenerContainer = new ReactiveRedisMessageListenerContainer(connectionFactory);
        ChannelTopic channelTopic = new ChannelTopic(channel);
        this.keyPrefix = keyPrefix;
        this.keyPrefixMaterial=keyPrefixMaterial;
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
        return redisTemplate.keys(keyPrefix + "*")
                .flatMap(redisTemplate::delete)
                .thenMany(
                        remisionRepository.findAll()
                                .flatMap(remision -> {
                                    String redisKey = keyPrefix + remision.getId();
                                    try {
                                        String json = objectMapper.writeValueAsString(remision);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return Mono.error(new RuntimeException("Error serializando remision", e));
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    sseService.publish("REFRESH_REMISIONES");
                    log.debug("Cache de remisiones refrescada desde SQL Server");
                    refreshAllMateriales().subscribe();
                });
    }

    public Mono<Void> refreshAllMateriales() {
        return redisTemplate.keys(keyPrefixMaterial + "*")
                .flatMap(redisTemplate::delete)
                .thenMany(
                        remisionMaterialRepository.findAll()
                                .flatMap(remisionMaterial -> {
                                    String redisKey = keyPrefixMaterial + remisionMaterial.getId();
                                    try {
                                        String json = objectMapper.writeValueAsString(remisionMaterial);
                                        return redisTemplate.opsForValue().set(redisKey, json);
                                    } catch (JsonProcessingException e) {
                                        return Mono.error(new RuntimeException("Error serializando proveedor", e));
                                    }
                                })
                )
                .then()
                .doOnSuccess(v -> {
                    sseService.publish("REFRESH_REMISION_MATERIAL");
                    log.debug("Cache de remisiones materiales refrescada desde SQL Server");
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
