package com.iisigroup.df.cache;

import com.iisigroup.df.design_pattern.Identifiable;

import java.util.Map;

public interface CacheKiller extends Identifiable<CacheKillerId> {
    // 清除 cache 實做方法
    void kill(Map<String, Object> spec);

}
