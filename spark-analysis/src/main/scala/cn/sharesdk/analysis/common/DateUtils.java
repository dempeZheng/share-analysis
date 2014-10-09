package cn.sharesdk.analysis.common;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by dempe on 14-5-27.
 */
public class DateUtils {

    private final static int TIMENUM = 1000;
    public static DateFormat format = new SimpleDateFormat("yyyyMMdd");
    public static int dateArr[] = {1, 2, 3, 4, 5, 6, 7, 14, 30};
    public static int[] dailyRetention = {1, 2, 3, 4, 5, 6, 7, 14, 30};
    private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static Date formatStrDate(String strDate) {
        Date date = null;
        try {
            date = sdf.parse(strDate);
        } catch (Exception e) {
            date = new Date();
        }
        return date;
    }

    public static long getDatePeriod(String startDate, String endDate) {
        return (formatStrDate(endDate).getTime() - formatStrDate(startDate).getTime()) / TIMENUM;
    }

    public static String getCurMonth() {
        return format.format(new Date()).substring(0, 8);
    }

    /**
     * @param dateStr
     * @param regex
     * @return
     */
    public static String formatStr(String dateStr, String regex) {
        String year = dateStr.substring(0, 4);
        String month = dateStr.substring(5, 7);
        String day = dateStr.substring(8, 10);
        String hour = dateStr.substring(11, 13);
        String min = dateStr.substring(14, 16);
        String sec = dateStr.substring(17, 19);
        regex = regex.toUpperCase();
        return regex.replace("YYYY", year).replace("MM", month).replace("DD", day).replace("HH", hour).replace("MI", min).
                replace("SS", sec);
    }

    public static int[] getRetentionFreq(String date1, String date2) throws Exception {
        int arr[] = new int[3];
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(format.parse(date1));
        int day1 = calendar.get(Calendar.DAY_OF_YEAR);

        Calendar calendar2 = Calendar.getInstance();
        calendar2.setTime(format.parse(date2));
        int day2 = calendar2.get(Calendar.DAY_OF_YEAR);
        int retentionDay = day1 - day2;


        int week1 = calendar.get(Calendar.WEEK_OF_YEAR);
        int week2 = calendar2.get(Calendar.WEEK_OF_YEAR);
        int retentionWeek = week1 - week2;

        int month1 = calendar.get(Calendar.MONTH);
        int month2 = calendar2.get(Calendar.MONTH);
        int retentionMonth = month1 - month2;


        retentionDay = getRpFreq(dailyRetention, retentionDay);
        retentionWeek = getRetentionFreq(retentionWeek);
        retentionMonth = getRetentionFreq(retentionMonth);

        arr[0] = retentionDay;
        arr[1] = retentionWeek;
        arr[2] = retentionMonth;
        return arr;

    }

    private static int getRpFreq(int arr[], int a) {
        for (int m : arr) {
            if (m == a) {
                return m;
            }
        }
        return 0;
    }

    private static int getRetentionFreq(int a) {
        if (a < 9 && a > 0) {
            return a;
        }
        return 0;
    }


}
