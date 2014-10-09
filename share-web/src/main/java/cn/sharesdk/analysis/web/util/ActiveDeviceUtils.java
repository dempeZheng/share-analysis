package cn.sharesdk.analysis.web.util;

import cn.sharesdk.analysis.web.Constants;
import redis.clients.jedis.Jedis;

import java.util.Set;

/**
 * Created by dempe on 14-6-17.
 */
public class ActiveDeviceUtils {

    public static int getActiveDevice(String appkey, String version, String channel, String createDate) {
        String key = "ad" + Constants.SPLIT + appkey + Constants.SPLIT + version + Constants.SPLIT + channel +
                Constants.SPLIT + createDate+Constants.SPLIT+"*";
        Jedis jedis = RedisUtil.getJedis();
        Set<String> hkeys = jedis.keys(key);
        int size =  hkeys.size();
        RedisUtil.returnJedis(jedis);
        return size;


    }
}
