package cn.sharesdk.analysis.web;

import cn.sharesdk.analysis.web.report.*;
import cn.sharesdk.analysis.web.report.Error;

/**
 * Created by root on 14-5-20.
 */
public class HttpServerBootstrap {

    public static void main(String[] args) {

        ActionRegistry registry = new ActionRegistry();

        registry.mapping("/" + Constants.REPORT_SUM_DAILY, new DailyReport());
        registry.mapping("/" + Constants.REPORT_SUM_DAILY_VERSION_CHANNEL, new DailyVersionChannel());
        registry.mapping("/" + Constants.REPORT_SUM_HOURLY, new Hourly());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_MODEL, new DeviceModel());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_SCREENSIZE, new DeviceScreensize());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_SYSVER, new DeviceSysver());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_NETWORKTYPE, new DeviceNetworktype());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_CARRIER, new DeviceCarrier());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_COUNTRY, new DeviceCountry());
        registry.mapping("/" + Constants.REPORT_SUM_DEVICE_PROVINCE, new DeviceProvince());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_PAGEPATH, new BrowsingPagepath());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_PATHDETAIL, new BrowsingPagedetail());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_FREQUENCY, new BrowsingFrequency());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_DURATION, new BrowsingDuration());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_PAGECOUNT, new BrowsingPagecount());
        registry.mapping("/" + Constants.REPORT_SUM_BROWSING_INTERVAL, new BrowsingInterval());
        registry.mapping("/" + Constants.REPORT_SUM_EVENT, new Event());
        registry.mapping("/" + Constants.REPORT_SUM_EVENT_KEYVALUE, new EventKeyvalue());
        registry.mapping("/" + Constants.REPORT_SUM_ERROR, new Error());
        registry.mapping("/" + Constants.REPORT_SUM_RETENTION_DAILY, new RetentionDaily());
        registry.mapping("/" + Constants.REPORT_SUM_RETENTION_WEEKLY, new RetentionWeekly());
        registry.mapping("/" + Constants.REPORT_SUM_RETENTION_MONTHLY, new RetentionMonthly());

        HttpAPIServer server = new HttpAPIServer(registry, "0.0.0.0", 8899);
        server.startup();
    }
}
