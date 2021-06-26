package com.wmy.orderPay.beans;

/**
 * ClassName:com.wmy.orderPay.beans.OrderEvent
 * Package:PACKAGE_NAME
 *
 * @date:2021/6/25 17:20
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 订单事件
 */
public class OrderEvent {
    private Long orderId;
    private String eventType;
    private String txId; // 交易码
    private Long timestamp;

    public OrderEvent() {
    }

    public OrderEvent(Long orderId, String eventType, String txId, Long timestamp) {
        this.orderId = orderId;
        this.eventType = eventType;
        this.txId = txId;
        this.timestamp = timestamp;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public String getTxId() {
        return txId;
    }

    public void setTxId(String txId) {
        this.txId = txId;
    }

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public String toString() {
        return "com.wmy.orderPay.beans.OrderEvent{" +
                "orderId=" + orderId +
                ", eventType='" + eventType + '\'' +
                ", txId='" + txId + '\'' +
                ", timestamp=" + timestamp +
                '}';
    }
}
