package cn.sharesdk.analysis.web;

import cn.sharesdk.analysis.web.util.DateUtils;
import cn.sharesdk.analysis.web.util.RedisUtil;
import com.lamfire.json.JSON;
import com.lamfire.json.JSONArray;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by liss on 14-5-27.
 */
public class ReportAction implements Action {

    public static String getDateRegx(String startDate, String endDate) {
        if (startDate.equals(endDate)) {
            // 日期相同
            return startDate;
        }
        String startYear = startDate.substring(0, 4);
        if (endDate.startsWith(startYear)) {
            // 年份相同
            String startMonth = startDate.substring(4, 6);
            String endMonth = endDate.substring(4, 6);
            if (startMonth.equals(endMonth)) {
                // 月份相同，日期不同，查询一个月
                return startYear + startMonth + "??";
            } else {
                // 月份不同
                return startYear + "????";
            }
        } else {
            return "*";
        }
    }

    public static String getPageCountSegment(long count) {
        if (count >= 1 && count <= 2) {
            return "1-2";
        } else if (count > 2 && count <= 5) {
            return "3-5";
        } else if (count > 5 && count <= 9) {
            return "6-9";
        } else if (count > 9 && count <= 29) {
            return "10-29";
        } else if (count > 29 && count <= 99) {
            return "30-99";
        } else if (count > 99) {
            return "100+";
        }
        return null;
    }

    /**
     * 获取参数.
     *
     * @param param
     * @return
     */
    private String getParameter(Map<String, List<String>> params, String param) {
        List<String> paramList = params.get(param);
        if (paramList != null && paramList.size() > 0) {
            return paramList.get(0);
        } else {
            if ("startDate".equals(param) || "endDate".equals(param)) {
                return null;
            } else {
                return "*";
            }
        }
    }

    /**
     * 请求分发.
     *
     * @param reportName
     * @param appkey
     * @return
     */
    private byte[] dispatch(String reportName, String appkey, Map<String, List<String>> params) {
        JSON json = new JSON();

        if (appkey == null || reportName == null) {
            json.put("status", 404);
            json.put("res", "Invalid param!");
        } else {
            if (Constants.REPORT_SUM_DAILY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDailyReport(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DAILY_VERSION_CHANNEL.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDailyVersionChannel(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_HOURLY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumHourly(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_MODEL.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceModel(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_SCREENSIZE.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceScreensize(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_SYSVER.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceSysver(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_NETWORKTYPE.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceNetworktype(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_CARRIER.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceCarrier(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_COUNTRY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceCountry(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_DEVICE_PROVINCE.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumDeviceProvince(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_PAGEPATH.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingPagepath(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_PATHDETAIL.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingPathdetail(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_EVENT.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumEvent(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_EVENT_KEYVALUE.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumEventKeyvalue(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_ERROR.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumError(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_INTERVAL.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingInterval(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_PAGECOUNT.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingPagecount(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_FREQUENCY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingFrequency(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_BROWSING_DURATION.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumBrowsingDuration(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_RETENTION_DAILY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumRetentionDaily(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_RETENTION_WEEKLY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumRetentionWeekly(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else if (Constants.REPORT_SUM_RETENTION_MONTHLY.equals(reportName)) {
                JSONArray ja = null;
                try {
                    ja = sumRetentionMonthly(appkey, params);

                    json.put("status", 200);
                    json.put("res", ja);
                } catch (Exception e) {
                    e.printStackTrace();

                    json.put("status", 404);
                    json.put("res", "Invalid param!");
                }
            } else {
                json.put("status", 404);
                json.put("res", "Invalid param!");
            }
        }

        return json.toBytes();
    }

    /**
     * **************** 报表处理 ***************************************
     */

    @Override
    public byte[] execute(ActionContext context, byte[] message, Map<String, List<String>> params) {
        String reportName = getParameter(params, "report_name");
        String appkey = getParameter(params, "appkey");
        return dispatch(reportName, appkey, params);
    }

    /**
     * sum_daily report.
     * <p/>
     * <br/>
     * url :
     * report?report_name=sum_daily&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140528&endDate=20140531
     *
     * @param _appkey
     * @return
     */
    private JSONArray sumDailyReport(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String key = "sum_daily/" + _appkey + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Iterator<String> it = hkeys.iterator();
            Pipeline pipeline = jedis.pipelined();
            while (it.hasNext()) {
                JSON map = new JSON();
                String hkey = (String) it.next();
                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[2]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                map.put("appkey", cols[1]);
                map.put("create_date", cols[2]);

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device", "active_device", "duration", "jailbroken_num", "pirate_num");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();
                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);
                String active_device = fieldValues.get(2);
                String duration = fieldValues.get(3);
                String jailbroken_num = fieldValues.get(4);
                String pirate_num = fieldValues.get(5);

                map.put("run_num", run_num);
                map.put("new_device", new_device);
                map.put("active_device", active_device);
                map.put("duration", duration);
                map.put("jailbroken_num", jailbroken_num);
                map.put("pirate_num", pirate_num);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_daily_version_channel
     * <p/>
     * <br/>
     * url : report?report_name=sum_daily_version_channel&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140528&endDate=20140530&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDailyVersionChannel(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_daily_version_channel/" + _appkey + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[4]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device", "active_device", "duration");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);
                String active_device = fieldValues.get(2);
                String duration = fieldValues.get(3);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("create_date", cols[4]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);
                map.put("active_device", active_device);
                map.put("duration", duration);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_hourly
     * <p/>
     * <br/>
     * url :
     * report?report_name=sum_hourly&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&create_hour=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumHourly(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _create_hour = getParameter(params, "create_hour");

        String key = "sum_hourly/" + _appkey + "/" + _create_date + "/" + _create_hour;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[2]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("create_date", cols[2]);
                map.put("create_hour", cols[3]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_model
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_model&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&model=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceModel(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _model = getParameter(params, "model");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_model/" + _appkey + "/" + _model + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("model", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_screensize
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_screensize&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&screensize=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceScreensize(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _screensize = getParameter(params, "screensize");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_screensize/" + _appkey + "/" + _screensize + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("screensize", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_sysver
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_sysver&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&sysver=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceSysver(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _sysver = getParameter(params, "sysver");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_sysver/" + _appkey + "/" + _sysver + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("sysver", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_networktype
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_networktype&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&networktype=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceNetworktype(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _networktype = getParameter(params, "networktype");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_networktype/" + _appkey + "/" + _networktype + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("networktype", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_carrier
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_carrier&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&carrier=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceCarrier(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _carrier = getParameter(params, "carrier");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_carrier/" + _appkey + "/" + _carrier + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("carrier", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_country
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_country&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&country=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceCountry(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _country = getParameter(params, "country");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_country/" + _appkey + "/" + _country + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("country", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_device_province
     * <p/>
     * <br/>
     * url : report?report_name=sum_device_province&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&province=*&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumDeviceProvince(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _province = getParameter(params, "province");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_device_province/" + _appkey + "/" + _province + "/" + _version + "/" + _channel + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "new_device");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String new_device = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("province", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("create_date", cols[5]);
                map.put("run_num", run_num);
                map.put("new_device", new_device);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_browsing_pagepath
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_pagepath&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&page=*&version=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingPagepath(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _page = getParameter(params, "page");
        String _version = getParameter(params, "version");

        String key = "sum_browsing_pagepath/" + _appkey + "/" + _page + "/" + _version + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[4]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "num", "exit_num", "total_time");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String num = fieldValues.get(0);
                String exit_num = fieldValues.get(1);
                String total_time = fieldValues.get(2);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("page", cols[2]);
                map.put("version", cols[3]);
                map.put("create_date", cols[4]);
                map.put("num", num);
                map.put("exit_num", exit_num);
                map.put("total_time", total_time);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_browsing_pathdetail
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_pathdetail&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&page=*&from_page=*&version=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingPathdetail(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _page = getParameter(params, "page");
        String _from_page = getParameter(params, "from_page");
        String _version = getParameter(params, "version");

        String key = "sum_browsing_pathdetail/" + _appkey + "/" + _version + "/" + _create_date + "/" + _page + "/" + _from_page;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[3]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                String num = jedis.hget(hkey, "num");

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("create_date", cols[3]);
                map.put("page", cols[4]);
                map.put("from_page", cols[5]);
                map.put("num", num);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_event
     * <p/>
     * <br/>
     * url :
     * report?report_name=sum_event&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*&eventkey=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumEvent(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _eventkey = getParameter(params, "eventkey");
        String _channel = getParameter(params, "channel");
        String _version = getParameter(params, "version");

        String key = "sum_event/" + _appkey + "/" + _version + "/" + _channel + "/" + _eventkey + "/" + _create_date;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "notice_num", "run_num", "device_num", "duration");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String notice_num = fieldValues.get(0);
                String run_num = fieldValues.get(1);
                String device_num = fieldValues.get(2);
                String duration = fieldValues.get(3);

                JSON map = new JSON();
                map.put("create_date", cols[5]);
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("eventkey", cols[4]);
                map.put("notice_num", notice_num);
                map.put("run_num", run_num);
                map.put("device_num", device_num);
                map.put("duration", duration);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_event_keyvalue
     * <p/>
     * <br/>
     * url : report?report_name=sum_event_keyvalue&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*&eventkey=*&key=*&value=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumEventKeyvalue(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _eventkey = getParameter(params, "eventkey");
        String _channel = getParameter(params, "channel");
        String _version = getParameter(params, "version");
        String _key = getParameter(params, "key");
        String _value = getParameter(params, "value");
        String key = "sum_event_keyvalue/" + _appkey + "/" + _version + "/" + _channel + "/" + _eventkey + "/" + _create_date + "/" + _key + "/" + _value;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[5]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "notice_num", "duration");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String notice_num = fieldValues.get(0);
                String duration = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("eventkey", cols[4]);
                map.put("create_date", cols[5]);
                map.put("key", cols[6]);
                map.put("value", cols[7]);
                map.put("notice_num", notice_num);
                map.put("duration", duration);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_error
     * <p/>
     * <br/>
     * url :
     * report?report_name=sum_error&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&model=*&networktype=*&sysver=*&error_log=*&stack_trace=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumError(String _appkey, Map<String, List<String>> params) {
        Jedis jedis = RedisUtil.getJedis();
        JSONArray jsonArray = new JSONArray();
        try {
            String _startDate = getParameter(params, "startDate");
            String _endDate = getParameter(params, "endDate");

            int _istartDate = Integer.parseInt(_startDate);
            int _iendDate = Integer.parseInt(_endDate);

            String _create_date = getDateRegx(_startDate, _endDate);

            String _version = getParameter(params, "version");
            // String _model = getParameter(params, "model");
            String _model = "*";
            // String _networktype = getParameter(params, "networktype");
            String _networktype = "*";
            // String _sysver = getParameter(params, "sysver");
            String _sysver = "*";
            String _error_log = getParameter(params, "error_log");
            String _stack_trace = getParameter(params, "stack_trace");

            String key = "sum_error/" + _create_date + "/" + _appkey + "/" + _version + "/" + _model + "/" + _networktype + "/" + _sysver + "/" + _error_log + "/" + _stack_trace;
            Set<String> hkeys = jedis.keys(key);
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[1]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                String num = jedis.hget(hkey, "num");

                JSON map = new JSON();
                map.put("create_date", cols[1]);
                map.put("appkey", cols[2]);
                map.put("version", cols[3]);
                map.put("model", cols[4]);
                map.put("networktype", cols[5]);
                map.put("sysver", cols[6]);
                map.put("error_log", cols[7]);
                map.put("stack_trace", cols[8]);
                map.put("num", num);

                jsonArray.add(map);
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }
        return jsonArray;
    }

    /**
     * sum_browsing_interval
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_interval&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingInterval(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_browsing_interval/" + _appkey + "/" + _create_date + "/" + _version + "/" + _channel + "/*";

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[2]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                String run_num = jedis.hget(hkey, "run_num");

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("create_date", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("segment", cols[5]);
                map.put("run_num", run_num);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_browsing_pagecount
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_pagecount&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingPagecount(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");

        String key = "sum_browsing_pagecount/" + _appkey + "/" + _create_date + "/" + _version + "/" + _channel;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[2]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "segment");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String segment = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("create_date", cols[2]);
                map.put("version", cols[3]);
                map.put("channel", cols[4]);
                map.put("run_num", run_num);
                map.put("segment", getPageCountSegment(Long.parseLong(segment)));

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_browsing_frequency
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_frequency&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*&segment=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingFrequency(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");
        String _segment = getParameter(params, "segment");

        String key = "sum_browsing_frequency/" + _appkey + "/" + _version + "/" + _channel + "/" + _create_date + "/" + _segment;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[4]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                String device_num = jedis.hget(hkey, "device_num");

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("create_date", cols[4]);
                map.put("segment", cols[5]);
                map.put("device_num", device_num);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_browsing_duration
     * <p/>
     * <br/>
     * url : report?report_name=sum_browsing_duration&appkey=
     * b5558eab134cf306fdcdc3a6746afa25
     * &startDate=20140531&endDate=20140531&version=*&channel=*&segment=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumBrowsingDuration(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

        String _create_date = getDateRegx(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");
        String _segment = getParameter(params, "segment");

        String key = "sum_browsing_duration/" + _appkey + "/" + _version + "/" + _channel + "/" + _create_date + "/" + _segment;

        JSONArray jsonArray = new JSONArray();

        Jedis jedis = null;
        try {
            jedis = RedisUtil.getJedis();
            Set<String> hkeys = jedis.keys(key);
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                int createDate = Integer.parseInt(cols[4]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "run_num", "device_num");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String run_num = fieldValues.get(0);
                String device_num = fieldValues.get(1);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("create_date", cols[4]);
                map.put("segment", cols[5]);
                map.put("device_num", device_num);
                map.put("run_num", run_num);

                jsonArray.add(map);
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_retention_daily
     * <p/>
     * <br/>
     * url : report?report_name=sum_retention_daily&appkey=
     * b5558eab134cf306fdcdc3a6746afa25
     * &startDate=20140531&endDate=20140531&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumRetentionDaily(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");

        int _istartDate = Integer.parseInt(_startDate);
        int _iendDate = Integer.parseInt(_endDate);

		/*String _create_date = getDateRegx(_startDate, _endDate);*/
        String[] days = DateUtils.days(_startDate, _endDate);

        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");
        JSONArray jsonArray = new JSONArray();
        Jedis jedis = RedisUtil.getJedis();
        for (String day : days) {
            String key = "sum_retention_daily/" + _appkey + "/" + _version + "/" + _channel + "/" + day;

            Set<String> hkeys = jedis.keys(key);
            if (hkeys.size() <= 0) {
                JSON map = new JSON();
                map.put("appkey", _appkey);
                map.put("version", _version);
                map.put("channel", _channel);
                map.put("create_date", day);
                map.put("new_device", 0);
                map.put("next_day1", 0);
                map.put("next_day2", 0);
                map.put("next_day3", 0);
                map.put("next_day4", 0);
                map.put("next_day5", 0);
                map.put("next_day6", 0);
                map.put("next_day7", 0);
                map.put("next_day14", 0);
                map.put("next_day30", 0);
                jsonArray.add(map);
            }
            Pipeline pipeline = jedis.pipelined();
            Iterator<String> it = hkeys.iterator();
            while (it.hasNext()) {
                String hkey = (String) it.next();

                String[] cols = hkey.split("/");

                /*int createDate = Integer.parseInt(cols[4]);
                if (!(createDate >= _istartDate && createDate <= _iendDate)) {
                    continue;
                }*/

                Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "new_device", "next_day1", "next_day2", "next_day3", "next_day4", "next_day5", "next_day6", "next_day7", "next_day14", "next_day30");
                pipeline.sync();
                List<String> fieldValues = fieldValueResponse.get();

                String new_device = fieldValues.get(0);
                String next_day1 = fieldValues.get(1);
                String next_day2 = fieldValues.get(2);
                String next_day3 = fieldValues.get(3);
                String next_day4 = fieldValues.get(4);
                String next_day5 = fieldValues.get(5);
                String next_day6 = fieldValues.get(6);
                String next_day7 = fieldValues.get(7);
                String next_day14 = fieldValues.get(8);
                String next_day30 = fieldValues.get(9);

                JSON map = new JSON();
                map.put("appkey", cols[1]);
                map.put("version", cols[2]);
                map.put("channel", cols[3]);
                map.put("create_date", day);
                map.put("new_device", new_device);
                map.put("next_day1", next_day1);
                map.put("next_day2", next_day2);
                map.put("next_day3", next_day3);
                map.put("next_day4", next_day4);
                map.put("next_day5", next_day5);
                map.put("next_day6", next_day6);
                map.put("next_day7", next_day7);
                map.put("next_day14", next_day14);
                map.put("next_day30", next_day30);

                jsonArray.add(map);
            }
        }

        RedisUtil.returnJedis(jedis);

        return jsonArray;
    }

    /**
     * sum_retention_weekly
     * <p/>
     * <br/>
     * url : report?report_name=sum_retention_weekly&appkey=b5558eab134cf306fdcdc3a6746afa25&startDate=20140531&endDate=20140531&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumRetentionWeekly(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");
        // 获取时间段的周列表
        String[] weeks = DateUtils.dayWeeks(_startDate, _endDate);
        Jedis jedis = null;
        JSONArray jsonArray;
        try {
            jedis = RedisUtil.getJedis();
            jsonArray = new JSONArray();
            for (String week : weeks) {

                String key = "sum_retention_weekly/" + _appkey + "/" + _version + "/" + _channel + "/" + week;

                Set<String> hkeys = jedis.keys(key);
                Pipeline pipeline = jedis.pipelined();
                Iterator<String> it = hkeys.iterator();
                while (it.hasNext()) {
                    String hkey = (String) it.next();
                    String[] cols = hkey.split("/");

                    Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "new_device", "next_week1", "next_week2", "next_week3", "next_week4", "next_week5", "next_week6", "next_week7", "next_week8", "next_week9");
                    pipeline.sync();
                    List<String> fieldValues = fieldValueResponse.get();
                    String new_device = fieldValues.get(0);
                    String next_week1 = fieldValues.get(1);
                    String next_week2 = fieldValues.get(2);
                    String next_week3 = fieldValues.get(3);
                    String next_week4 = fieldValues.get(4);
                    String next_week5 = fieldValues.get(5);
                    String next_week6 = fieldValues.get(6);
                    String next_week7 = fieldValues.get(7);
                    String next_week8 = fieldValues.get(8);
                    String next_week9 = fieldValues.get(9);

                    JSON map = new JSON();
                    map.put("appkey", cols[1]);
                    map.put("version", cols[2]);
                    map.put("channel", cols[3]);
                    map.put("create_date", week);
                    map.put("new_device", new_device);
                    map.put("next_week1", next_week1);
                    map.put("next_week2", next_week2);
                    map.put("next_week3", next_week3);
                    map.put("next_week4", next_week4);
                    map.put("next_week5", next_week5);
                    map.put("next_week6", next_week6);
                    map.put("next_week7", next_week7);
                    map.put("next_week8", next_week8);
                    map.put("next_week9", next_week9);

                    jsonArray.add(map);
                }
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }

    /**
     * sum_retention_monthly
     * <p/>
     * <br/>
     * url : report?report_name=sum_retention_monthly&appkey=
     * b5558eab134cf306fdcdc3a6746afa25
     * &startDate=20140531&endDate=20140531&version=*&channel=*
     *
     * @param _appkey
     * @param params
     * @return
     */
    private JSONArray sumRetentionMonthly(String _appkey, Map<String, List<String>> params) {
        String _startDate = getParameter(params, "startDate");
        String _endDate = getParameter(params, "endDate");
        String _version = getParameter(params, "version");
        String _channel = getParameter(params, "channel");
        // 获取时间段的月列表
        String[] months = DateUtils.dayMonths(_startDate, _endDate);
        Jedis jedis = null;
        JSONArray jsonArray;
        try {
            jedis = RedisUtil.getJedis();
            jsonArray = new JSONArray();
            for (String month : months) {
                String key = "sum_retention_monthly/" + _appkey + "/" + _version + "/" + _channel + "/" + month;

                Set<String> hkeys = jedis.keys(key);
                Pipeline pipeline = jedis.pipelined();
                Iterator<String> it = hkeys.iterator();
                while (it.hasNext()) {
                    String hkey = (String) it.next();

                    String[] cols = hkey.split("/");

                    Response<List<String>> fieldValueResponse = pipeline.hmget(hkey, "new_device", "next_month1", "next_month2", "next_month3", "next_month4", "next_month5", "next_month6", "next_month7", "next_month8", "next_month9");
                    pipeline.sync();
                    List<String> fieldValues = fieldValueResponse.get();

                    String new_device = fieldValues.get(0);
                    String next_month1 = fieldValues.get(1);
                    String next_month2 = fieldValues.get(2);
                    String next_month3 = fieldValues.get(3);
                    String next_month4 = fieldValues.get(4);
                    String next_month5 = fieldValues.get(5);
                    String next_month6 = fieldValues.get(6);
                    String next_month7 = fieldValues.get(7);
                    String next_month8 = fieldValues.get(8);
                    String next_month9 = fieldValues.get(9);

                    JSON map = new JSON();
                    map.put("appkey", cols[1]);
                    map.put("version", cols[2]);
                    map.put("channel", cols[3]);
                    map.put("create_date", month);
                    map.put("new_device", new_device);
                    map.put("next_month1", next_month1);
                    map.put("next_month2", next_month2);
                    map.put("next_month3", next_month3);
                    map.put("next_month4", next_month4);
                    map.put("next_month5", next_month5);
                    map.put("next_month6", next_month6);
                    map.put("next_month7", next_month7);
                    map.put("next_month8", next_month8);
                    map.put("next_month9", next_month9);

                    jsonArray.add(map);
                }
            }
        } finally {
            if (jedis != null) {
                RedisUtil.returnJedis(jedis);
            }
        }

        return jsonArray;
    }
}
