package com.wmy.market_analysis.beans;

/**
 * ClassName:BlackListUserWarning
 * Package:com.wmy.market_analysis.beans
 *
 * @date:2021/6/25 11:03
 * @author:数仓开发工程师
 * @email:2647716549@qq.com
 * @Description: 刷单行为POJO
 */
public class BlackListUserWarning {
    private Long userId;
    private Long adId;
    private String warningMsg;

    public BlackListUserWarning() {
    }

    public BlackListUserWarning(Long userId, Long adId, String warningMsg) {
        this.userId = userId;
        this.adId = adId;
        this.warningMsg = warningMsg;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public Long getAdId() {
        return adId;
    }

    public void setAdId(Long adId) {
        this.adId = adId;
    }

    public String getWarningMsg() {
        return warningMsg;
    }

    public void setWarningMsg(String warningMsg) {
        this.warningMsg = warningMsg;
    }

    @Override
    public String toString() {
        return "BlackListUserWarning{" +
                "userId=" + userId +
                ", adId=" + adId +
                ", warningMsg='" + warningMsg + '\'' +
                '}';
    }
}
