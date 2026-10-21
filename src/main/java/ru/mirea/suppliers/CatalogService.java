package ru.mirea.suppliers;

import java.math.BigDecimal;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import static ru.mirea.suppliers.Models.*;

@Service
@Profile("server")
public class CatalogService {
    private final List<Darkstore> darkstores = List.of(
        new Darkstore(1, "Москва, ул. Складская, 1"), new Darkstore(2, "Казань, ул. Складская, 2"));
    private final List<Good> products = List.of(
        new Good(1, "Пицца \"Маргарита\"", new BigDecimal("599.99")),
        new Good(2, "Салат \"Цезарь\"", new BigDecimal("249.99")),
        new Good(3, "Бургер \"Гриль\"", new BigDecimal("399.99")),
        new Good(4, "Суп \"Минестроне\"", new BigDecimal("199.99")),
        new Good(5, "Десерт \"Тирамису\"", new BigDecimal("149.99")),
        new Good(6, "Лазанья \"Болоньезе\"", new BigDecimal("499.99")),
        new Good(7, "Крем-суп из шампиньонов", new BigDecimal("249.99")),
        new Good(8, "Греческий салат", new BigDecimal("299.99")),
        new Good(9, "Паста \"Карбонара\"", new BigDecimal("349.99")),
        new Good(10, "Яблочный пирог", new BigDecimal("199.99")));
    private final Map<Long, Delivery> deliveries = new ConcurrentHashMap<>(Map.of(
        1L, new Delivery(1, "2026-10-21T10:00:00", darkstores.get(0), DeliveryStatus.SUBMITTED),
        2L, new Delivery(2, "2026-10-22T14:00:00", darkstores.get(1), DeliveryStatus.ASSEMBLY)));
    private final Map<Long, List<GoodsList>> positions = new ConcurrentHashMap<>(Map.of(
        1L, List.of(new GoodsList(products.get(0), 2), new GoodsList(products.get(1), 1)),
        2L, List.of(new GoodsList(products.get(2), 3), new GoodsList(products.get(3), 5))));

    public Mono<Good> product(long id) {
        return Mono.defer(() -> Mono.justOrEmpty(products.stream().filter(p -> p.id() == id).findFirst())
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Товар " + id + " не найден"))));
    }
    public Mono<Darkstore> darkstore(long id) {
        return Mono.defer(() -> Mono.justOrEmpty(darkstores.stream().filter(s -> s.id() == id).findFirst())
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Склад " + id + " не найден"))));
    }
    public Mono<Delivery> delivery(long id) {
        return Mono.defer(() -> Mono.justOrEmpty(deliveries.get(id))
            .switchIfEmpty(Mono.error(new IllegalArgumentException("Доставка " + id + " не найдена"))));
    }
    public Flux<GoodsList> goodsList(long deliveryId) {
        return delivery(deliveryId).flatMapMany(d -> Flux.fromIterable(positions.getOrDefault(d.id(), List.of())))
            .delayElements(Duration.ofMillis(400));
    }
    public Flux<Good> stream(CatalogRequest request) {
        return request.deliveryId() == null
            ? Flux.fromIterable(products).delayElements(Duration.ofMillis(400))
            : goodsList(request.deliveryId()).map(GoodsList::good);
    }
    // Delivery.tsx: редактирование статуса заказа через PUT /delivery.
    public Mono<Void> changeStatus(DeliveryStatusEvent event) {
        return delivery(event.deliveryId()).doOnNext(d -> {
            if (event.status() == null) throw new IllegalArgumentException("Не выбран статус заказа");
            deliveries.computeIfPresent(d.id(), (id, current) -> new Delivery(id, current.deliveryDatetime(),
                current.darkstore(), event.status()));
        }).then();
    }
    // GoodsList.tsx: «Изменить количество», PUT /goodslist (deliveryId, goodId, count).
    public Mono<GoodsListResult> updatePosition(GoodsListUpdate request) {
        return Mono.defer(() -> {
            if (request.count() <= 0) return Mono.error(new IllegalArgumentException("Количество должно быть больше нуля"));
            return delivery(request.deliveryId()).then(product(request.goodId())).map(good -> {
                positions.compute(request.deliveryId(), (id, rows) -> {
                    if (rows == null || rows.stream().noneMatch(row -> row.good().id() == good.id())) {
                        throw new IllegalArgumentException("Товар " + good.id() + " отсутствует в доставке " + id);
                    }
                    return rows.stream().map(row -> row.good().id() == good.id()
                        ? new GoodsList(good, request.count()) : row).toList();
                });
                return new GoodsListResult(request.deliveryId(), good.id(), request.count(), "OK", "Количество изменено");
            });
        }).onErrorResume(IllegalArgumentException.class, error -> Mono.just(
            new GoodsListResult(request.deliveryId(), request.goodId(), request.count(), "ERROR", error.getMessage())));
    }
}
