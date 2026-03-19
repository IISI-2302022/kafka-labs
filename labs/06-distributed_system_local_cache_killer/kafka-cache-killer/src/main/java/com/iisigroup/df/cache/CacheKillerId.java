package com.iisigroup.df.cache;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class CacheKillerId {
    // 種類
    private String kind;
    // 名稱
    private String name;

    public static CacheKillerId with(String kind, String name) {
        return new CacheKillerId(kind, name);
    }
}
