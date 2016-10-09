package com.xunlei.redis.clients.util;

/**
 * Created by lvfei on 16/10/9.
 */
public class StringUtil {
    public static boolean isNullOrEmpty(String str) {
        if (str == null || str.isEmpty()) {
            return true;
        }
        return false;
    }
}
