package com.macroflag.plusplatform.front.manager.Serial.impl;

import com.macroflag.plusplatform.common.cache.RedisCache;
import com.macroflag.plusplatform.front.manager.Serial.Serial;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component("serial")
public class SerialImpl implements Serial {

    @Autowired
    private RedisCache redisCache;

    final private String runningKey = "serial:running:";
    final private Long tTl = 60 * 60L;
    final private String readyKey = "serial:ready:";

    @Override
    public void run(String... strings) throws Exception {
        if (!redisCache.setNxEx("init:sysStart", "1", 5 * 60))
            return;
        List<String> keyList = redisCache.keys(runningKey + "*");
        keyList.parallelStream().forEach(it -> {
            redisCache.rPopLPushEx(it.substring(it.indexOf("_") + 1), readyKey + it.substring(it.indexOf("pro")), tTl);
            if (redisCache.exists(it))
                redisCache.deleteCache(it);
        });
    }


    @Override
    public boolean init(String productNo, String phone) {
        String suffix = productNo + ":" + phone;
        if (!redisCache.setNxEx("init:" + suffix, "1", 5L))
            return false;
        if (redisCache.exists(runningKey + suffix, readyKey + suffix)) {
            String result = redisCache.bLPopRPush(readyKey + suffix, runningKey + suffix, 30);
            if (null == result) {
                redisCache.deleteCache("init:" + suffix);
                return false;
            }
            redisCache.deleteCache("init:" + suffix);
            return true;
        }
        Long pushR = redisCache.lPush(runningKey + suffix, "1");
        if (null == pushR) {
            redisCache.deleteCache("init:" + suffix);
            return false;
        }
        if (1 != pushR) {
            redisCache.deleteCache(runningKey + suffix);
            redisCache.deleteCache("init:" + suffix);
            return false;
        }
        redisCache.deleteCache("init:" + suffix);
        return true;
    }

    @Override
    public boolean entrance(String productNo, String phone) {
        String suffix = productNo + ":" + phone;
        String result = redisCache.bLPopRPush(readyKey + suffix, runningKey + suffix, 30);
        if (null == result)
            return false;
        return true;
    }

    @Override
    public boolean exit(String productNo, String phone) {
        String suffix = productNo + ":" + phone;
        String result = redisCache.rPopLPushEx(runningKey + suffix, readyKey + suffix, tTl);
        if (null == result)
            return false;
        return true;
    }
}
