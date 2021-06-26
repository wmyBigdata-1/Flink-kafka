package com.wmy.market_analysis.beans;

/**
 * ClassName:AdCountViewByProvince
 * Package:com.wmy.market_analysis.beans
 *
 * @date:2021/6/25 9:41
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 广告点击量 --->
 */
public class AdCountViewByProvince {
    private String province; // 省份
    private String windowEnd; // 窗口结束事件
    private Long count; // 广告投放数量

    public AdCountViewByProvince() {
    }

    public AdCountViewByProvince(String province, String windowEnd, Long count) {
        this.province = province;
        this.windowEnd = windowEnd;
        this.count = count;
    }

    public String getProvince() {
        return province;
    }

    public void setProvince(String province) {
        this.province = province;
    }

    public String getWindowEnd() {
        return windowEnd;
    }

    public void setWindowEnd(String windowEnd) {
        this.windowEnd = windowEnd;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    @Override
    public String toString() {
        return "AdCountViewByProvince{" +
                "province='" + province + '\'' +
                ", windowEnd='" + windowEnd + '\'' +
                ", count=" + count +
                '}';
    }
}
