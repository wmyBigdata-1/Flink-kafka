package com.wmy.orderPay.beans;

/**
 * ClassName:OrderResult
 * Package:com.wmy.orderPay.beans
 *
 * @date:2021/6/25 17:24
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 订单的结果状态
 */
public class OrderResult {
    private Long orderId;
    private String resultState;

    public OrderResult() {
    }

    public OrderResult(Long orderId, String resultState) {
        this.orderId = orderId;
        this.resultState = resultState;
    }

    public Long getOrderId() {
        return orderId;
    }

    public void setOrderId(Long orderId) {
        this.orderId = orderId;
    }

    public String getResultState() {
        return resultState;
    }

    public void setResultState(String resultState) {
        this.resultState = resultState;
    }

    @Override
    public String toString() {
        return "OrderResult{" +
                "orderId=" + orderId +
                ", resultState='" + resultState + '\'' +
                '}';
    }
}
