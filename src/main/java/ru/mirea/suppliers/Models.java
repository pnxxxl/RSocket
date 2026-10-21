package ru.mirea.suppliers;

import java.math.BigDecimal;

public final class Models {
    private Models() { }
    public record Darkstore(long id, String address) { }
    public record Good(long id, String name, BigDecimal price) { }
    public enum DeliveryStatus { SUBMITTED, ASSEMBLY, DELIVERY, COMPLETED, CANCELLED }
    public record Delivery(long id, String deliveryDatetime, Darkstore darkstore, DeliveryStatus status) { }
    public record GoodsList(Good good, int count) { }
    public record CatalogRequest(Long deliveryId) { }
    public record DeliveryStatusEvent(long deliveryId, DeliveryStatus status) { }
    public record GoodsListUpdate(long deliveryId, long goodId, int count) { }
    public record GoodsListResult(long deliveryId, long goodId, int count, String status, String message) { }
}
