package ru.mirea.suppliers;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.handler.annotation.MessageExceptionHandler;
import org.springframework.messaging.handler.annotation.MessageMapping;
import org.springframework.stereotype.Controller;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import java.time.Duration;
import static ru.mirea.suppliers.Models.*;

@Controller
@Profile("server")
public class CatalogController {
    private static final Logger log = LoggerFactory.getLogger(CatalogController.class);
    private final CatalogService catalog;
    public CatalogController(CatalogService catalog) { this.catalog = catalog; }

    @MessageMapping("goods.get")
    public Mono<Good> product(Long id) { return catalog.product(id); }
    @MessageMapping("darkstores.get")
    public Mono<Darkstore> darkstore(Long id) { return catalog.darkstore(id); }
    @MessageMapping("deliveries.get")
    public Mono<Delivery> delivery(Long id) { return catalog.delivery(id); }
    @MessageMapping("deliveries.goods")
    public Flux<GoodsList> goodsList(Long deliveryId) { return catalog.goodsList(deliveryId); }
    @MessageMapping("goods.stream")
    public Flux<Good> products(CatalogRequest request) { return catalog.stream(request); }
    @MessageMapping("deliveries.status")
    public Mono<Void> status(DeliveryStatusEvent event) {
        return catalog.changeStatus(event)
            .doOnSuccess(ignored -> log.info("Статус доставки {} изменён на {}", event.deliveryId(), event.status()))
            .onErrorResume(error -> {
                log.warn("Изменение статуса отклонено: {}", error.getMessage());
                return Mono.empty();
            });
    }
    @MessageMapping("goodslist.channel")
    public Flux<GoodsListResult> updatePositions(Flux<GoodsListUpdate> requests) {
        return requests.concatMap(request -> catalog.updatePosition(request).delayElement(Duration.ofMillis(200)));
    }
    @MessageExceptionHandler(IllegalArgumentException.class)
    public Mono<Void> invalidRequest(IllegalArgumentException error) {
        log.warn("Запрос отклонён: {}", error.getMessage());
        return Mono.error(error);
    }
}
