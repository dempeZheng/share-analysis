package cn.sharesdk.analysis;

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
        config.setMaxIdle(Integer.valueOf(Config.get(Constants.REDIS_POOL_MAXIDLE)));
        config.setMaxWaitMillis(Long.valueOf(Config.get(Constants.REDIS_POOL_MAXWAIT)));
        config.setTestOnBorrow(Boolean.valueOf(Config.get(Constants.REDIS_POOL_TESTONBORROW)));
        config.setTestOnReturn(Boolean.valueOf(Config.get(Constants.REDIS_POOL_TESTONRETURN)));
        String redisIp = Config.get(Constants.REDIS_IP);
        Integer redisPort = Integer.valueOf(Config.get(Constants.REDIS_PORT));
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
