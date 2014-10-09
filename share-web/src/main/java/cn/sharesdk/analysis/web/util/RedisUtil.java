package cn.sharesdk.analysis.web.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * operation redis
 *
 * @author Ron
 * @date 2014-5-23
 */
public class RedisUtil {


    private static JedisPool pool;


    /**
     * 构造实例redis 连接池.
     *
     */
    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxIdle(Integer.valueOf(ConfigUtils.getProperty("redis.pool.maxIdle")));
        config.setMaxWaitMillis(Long.valueOf(ConfigUtils.getProperty("redis.pool.maxWait")));
        config.setTestOnBorrow(Boolean.valueOf(ConfigUtils.getProperty("redis.pool.testOnBorrow")));
        config.setTestOnReturn(Boolean.valueOf(ConfigUtils.getProperty("redis.pool.testOnReturn")));
        String redisIp = ConfigUtils.getProperty("redis.ip");
        Integer redisPort = Integer.valueOf(ConfigUtils.getProperty("redis.port"));
        pool = new JedisPool(config, redisIp, redisPort);
    }

    /**
     * 获取jedis连接实例.
     *
     * @return
     */
    public static Jedis getJedis() {
        Jedis jedis = null;
        try {
            jedis = pool.getResource();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return jedis;
    }

    /**
     * 归还连接实例.
     */
    public static void returnJedis(Jedis jedis) {
        pool.returnResource(jedis);
    }

}
