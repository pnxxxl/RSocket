package ru.mirea.suppliers;

import java.time.Duration;
import java.time.LocalTime;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.messaging.rsocket.RSocketRequester;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import static ru.mirea.suppliers.Models.*;

@Component
@Profile("client")
public class DemoClient implements CommandLineRunner {
    private final RSocketRequester.Builder builder;
    private final String host;
    private final int port;
    public DemoClient(RSocketRequester.Builder builder, @Value("${demo.host}") String host,
                      @Value("${demo.port}") int port) {
        this.builder = builder; this.host = host; this.port = port;
    }
    private static void print(Object value) { System.out.printf("[%s] %s%n", LocalTime.now(), value); }
    @Override
    public void run(String... args) {
        RSocketRequester client = builder.tcp(host, port);
        try {
            Mono<Void> response = Mono.fromRunnable(() -> print("=== Request-Response ==="))
                .then(client.route("goods.get").data(1L).retrieveMono(Good.class))
                .doOnNext(DemoClient::print)
                .then(client.route("darkstores.get").data(1L).retrieveMono(Darkstore.class))
                .doOnNext(DemoClient::print)
                .then(client.route("deliveries.get").data(1L).retrieveMono(Delivery.class))
                .doOnNext(DemoClient::print).then();
            Mono<Void> missing = client.route("goods.get").data(999L).retrieveMono(Good.class)
                .doOnNext(DemoClient::print)
                .onErrorResume(error -> { print("Ожидаемая ошибка: " + error.getMessage()); return Mono.empty(); }).then();
            Mono<Void> stream = Mono.fromRunnable(() -> print("=== Request-Stream: один запрос, товары каждые ~400 мс ==="))
                .thenMany(client.route("goods.stream").data(new CatalogRequest(null)).retrieveFlux(Good.class)
                    .limitRate(1)
                    .doOnNext(p -> print("Получен товар №" + p.id() + " — " + p.price() + " ₽"))
                    .filter(p -> p.price().compareTo(new java.math.BigDecimal("300.00")) >= 0)
                    .map(p -> "Reactor: " + p.name().toUpperCase() + " — " + p.price() + " ₽")
                    .doOnNext(DemoClient::print)).then();
            Mono<Void> deliveryGoods = Mono.fromRunnable(() -> print("=== Состав доставки №1 (GoodsList из Bootcamp) ==="))
                .thenMany(client.route("deliveries.goods").data(1L).retrieveFlux(GoodsList.class)
                    .doOnNext(p -> print(p.good().name() + " — " + p.count() + " шт., цена " + p.good().price() + " ₽"))).then();
            Mono<Void> event = Mono.fromRunnable(() -> print("=== Fire-and-Forget: изменение статуса доставки №1 ==="))
                .then(client.route("deliveries.status").data(new DeliveryStatusEvent(1, DeliveryStatus.ASSEMBLY)).send())
                .doOnSuccess(ignored -> print("Событие отправлено; подтверждение обработки не запрашивалось"));
            Flux<GoodsListUpdate> outgoing = Flux.just(new GoodsListUpdate(1, 1, 4), new GoodsListUpdate(1, 999, 1),
                    new GoodsListUpdate(1, 2, 0), new GoodsListUpdate(1, 2, 3))
                .delayElements(Duration.ofMillis(500)).doOnNext(q -> print("Клиент → сервер: " + q));
            Mono<Void> channel = Mono.fromRunnable(() -> print("=== Channel: изменение количества товаров в доставке №1 ==="))
                .thenMany(client.route("goodslist.channel").data(outgoing, GoodsListUpdate.class)
                    .retrieveFlux(GoodsListResult.class).doOnNext(q -> print("Сервер → клиент: " + q))).then();
            Mono<Void> updated = Mono.fromRunnable(() -> print("=== Чтение доставки и состава после изменений ==="))
                .then(client.route("deliveries.get").data(1L).retrieveMono(Delivery.class)).doOnNext(DemoClient::print)
                .thenMany(client.route("deliveries.goods").data(1L).retrieveFlux(GoodsList.class)
                    .doOnNext(DemoClient::print)).then();
            response.then(missing).then(stream).then(deliveryGoods).then(event).then(channel).then(updated)
                .timeout(Duration.ofSeconds(30)).block();
            print("Демонстрация завершена");
        } catch (Exception error) {
            print("Ошибка соединения или выполнения: " + error.getMessage());
            throw new IllegalStateException("Не удалось завершить демонстрацию. Проверьте запуск сервера и адрес", error);
        } finally { client.dispose(); }
    }
}
