package ru.mirea.suppliers;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.rsocket.context.RSocketServerInitializedEvent;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.event.EventListener;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.messaging.rsocket.RSocketRequester;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static ru.mirea.suppliers.Models.*;

@SpringBootTest(properties = {"spring.profiles.active=server", "spring.rsocket.server.port=0"})
class RSocketIntegrationTest {
    @TestConfiguration
    static class Config {
        @Bean PortCapture portCapture() { return new PortCapture(); }
    }
    static class PortCapture {
        int port;
        @EventListener
        public void initialized(RSocketServerInitializedEvent event) {
            port = ((InetSocketAddress) event.getServer().address()).getPort();
        }
    }
    @Autowired PortCapture port;
    @Autowired RSocketRequester.Builder builder;
    @Autowired CatalogService catalog;
    RSocketRequester client;
    @BeforeEach void connect() {
        StepVerifier.setDefaultTimeout(Duration.ofSeconds(10));
        catalog.changeStatus(new DeliveryStatusEvent(1, DeliveryStatus.SUBMITTED)).block();
        catalog.updatePosition(new GoodsListUpdate(1, 1, 2)).block();
        catalog.updatePosition(new GoodsListUpdate(1, 2, 1)).block();
        client = builder.tcp("localhost", port.port);
    }
    @AfterEach void close() { client.dispose(); }

    @Test void requestResponseAndMissingObject() {
        StepVerifier.create(client.route("goods.get").data(1L).retrieveMono(Good.class))
            .assertNext(p -> {
                assertThat(p.name()).isEqualTo("Пицца \"Маргарита\"");
                assertThat(p.price()).isEqualByComparingTo("599.99");
            }).verifyComplete();
        StepVerifier.create(client.route("darkstores.get").data(1L).retrieveMono(Darkstore.class))
            .assertNext(s -> assertThat(s.address()).isEqualTo("Москва, ул. Складская, 1")).verifyComplete();
        StepVerifier.create(client.route("goods.get").data(999L).retrieveMono(Good.class))
            .expectErrorMatches(e -> e.getMessage().contains("не найден")).verify(Duration.ofSeconds(5));
        StepVerifier.create(client.route("darkstores.get").data(999L).retrieveMono(Darkstore.class))
            .expectErrorMatches(e -> e.getMessage().contains("не найден")).verify(Duration.ofSeconds(5));
    }
    @Test void streamArrivesGraduallyAndHonorsDemand() {
        StepVerifier.create(client.route("goods.stream").data(new CatalogRequest(null))
                .retrieveFlux(Good.class), 0)
            .thenRequest(1).expectNextMatches(p -> p.id() == 1)
            .expectNoEvent(Duration.ofMillis(150))
            .thenRequest(1).expectNextMatches(p -> p.id() == 2)
            .thenRequest(8).expectNextCount(8).verifyComplete();
    }
    @Test void streamCanBeFilteredAndRejectsMissingDelivery() {
        StepVerifier.create(client.route("goods.stream").data(new CatalogRequest(2L)).retrieveFlux(Good.class))
            .expectNextMatches(p -> p.id() == 3).expectNextMatches(p -> p.id() == 4).verifyComplete();
        StepVerifier.create(client.route("goods.stream").data(new CatalogRequest(999L)).retrieveFlux(Good.class))
            .expectErrorMatches(e -> e.getMessage().contains("не найдена")).verify(Duration.ofSeconds(5));
    }
    @Test void deliveryContainsDarkstoreAndStreamsNestedGoodsList() {
        StepVerifier.create(client.route("deliveries.get").data(1L).retrieveMono(Delivery.class))
            .assertNext(d -> {
                assertThat(d.status()).isEqualTo(DeliveryStatus.SUBMITTED);
                assertThat(d.darkstore().id()).isEqualTo(1);
                assertThat(d.deliveryDatetime()).isEqualTo("2026-10-21T10:00:00");
            }).verifyComplete();
        StepVerifier.create(client.route("deliveries.goods").data(1L).retrieveFlux(GoodsList.class))
            .expectNextMatches(p -> p.good().id() == 1 && p.count() == 2)
            .expectNextMatches(p -> p.good().id() == 2 && p.count() == 1).verifyComplete();
        StepVerifier.create(client.route("deliveries.get").data(999L).retrieveMono(Delivery.class))
            .expectErrorMatches(e -> e.getMessage().contains("не найдена")).verify(Duration.ofSeconds(5));
    }
    @Test void fireAndForgetIsProcessedOnServer() {
        client.route("deliveries.status").data(new DeliveryStatusEvent(1, DeliveryStatus.ASSEMBLY))
            .send().block(Duration.ofSeconds(5));
        await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
            assertThat(client.route("deliveries.get").data(1L).retrieveMono(Delivery.class)
                .block(Duration.ofSeconds(2)).status()).isEqualTo(DeliveryStatus.ASSEMBLY));
    }
    @Test void channelRespondsBeforeInputCompletesAndContinuesAfterInvalidItems() {
        AtomicBoolean inputComplete = new AtomicBoolean();
        Flux<GoodsListUpdate> requests = Flux.just(new GoodsListUpdate(1, 1, 4), new GoodsListUpdate(1, 999, 1),
            new GoodsListUpdate(1, 2, 0), new GoodsListUpdate(1, 2, 3)).delayElements(Duration.ofMillis(350))
            .doOnComplete(() -> inputComplete.set(true));
        StepVerifier.create(client.route("goodslist.channel").data(requests, GoodsListUpdate.class).retrieveFlux(GoodsListResult.class))
            .assertNext(q -> {
                assertThat(q.status()).isEqualTo("OK");
                assertThat(q.count()).isEqualTo(4);
                assertThat(inputComplete).isFalse();
            })
            .expectNextMatches(q -> q.status().equals("ERROR") && q.goodId() == 999)
            .expectNextMatches(q -> q.status().equals("ERROR") && q.count() == 0)
            .expectNextMatches(q -> q.status().equals("OK") && q.goodId() == 2 && q.count() == 3)
            .verifyComplete();
        StepVerifier.create(client.route("deliveries.goods").data(1L).retrieveFlux(GoodsList.class))
            .expectNextMatches(p -> p.good().id() == 1 && p.count() == 4)
            .expectNextMatches(p -> p.good().id() == 2 && p.count() == 3).verifyComplete();
    }
    @Test void invalidChannelChangesDoNotModifyDelivery() {
        Flux<GoodsListUpdate> invalid = Flux.just(new GoodsListUpdate(999, 1, 5), new GoodsListUpdate(1, 3, 5));
        StepVerifier.create(client.route("goodslist.channel").data(invalid, GoodsListUpdate.class).retrieveFlux(GoodsListResult.class))
            .expectNextMatches(r -> r.status().equals("ERROR") && r.message().contains("не найдена"))
            .expectNextMatches(r -> r.status().equals("ERROR") && r.message().contains("отсутствует"))
            .verifyComplete();
        StepVerifier.create(client.route("deliveries.goods").data(1L).retrieveFlux(GoodsList.class))
            .expectNextMatches(p -> p.good().id() == 1 && p.count() == 2)
            .expectNextMatches(p -> p.good().id() == 2 && p.count() == 1).verifyComplete();
    }
}
