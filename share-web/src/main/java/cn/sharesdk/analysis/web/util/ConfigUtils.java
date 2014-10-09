package cn.sharesdk.analysis.web.util;

import com.lamfire.utils.PropertiesUtils;

import java.util.Properties;

/**
 * Created by liss on 14-5-22.
 */
public class ConfigUtils {

    private static final String CONFIG_PATH = "/config.properties";

    public static String getProperty(String key) {
        Properties properties = new Properties();
        properties = PropertiesUtils.load(ConfigUtils.class.getResourceAsStream(CONFIG_PATH));
        return properties.getProperty(key);
    }
}
