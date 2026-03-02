package com.iisigroup.df.labs.constant;

/**
 * 共用常數定義類別。
 * <p>
 * 集中管理 Kafka Labs 中所有測試案例共用的常數值，
 * 避免硬編碼（hard-coded）字串散落在各個測試類別中。
 * </p>
 */
public final class Constants {

    /** 禁止實例化工具類別 */
    private Constants() {
        throw new RuntimeException();
    }

    /** 測試用的 Kafka Topic 名稱 */
    public static final String TEST_TOPIC = "test1";

    /** 訊息 Value 的前綴字串，用於產生測試資料（e.g. iisi0, iisi1, ...） */
    public static final String VALUE_PREFIX = "iisi";
}
