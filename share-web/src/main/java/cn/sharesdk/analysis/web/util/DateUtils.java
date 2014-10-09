package cn.sharesdk.analysis.web.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

/**
 * Created by liss on 14-6-5.
 */
public class DateUtils {

    public static String nextDay(String day) {
        try {
            Date date = com.lamfire.utils.DateUtils.parse(day, "yyyyMMdd");
            return format(com.lamfire.utils.DateUtils.addDays(date, 1));
        } catch (Exception e) {
            return null;
        }
    }

    public static String format(Date date) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return dateFormat.format(date);
    }

    public static String format(Calendar calendar) {
        DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
        return dateFormat.format(calendar.getTime());
    }


    public static String[] days(String sdate, String edate) {
        String[] days = new String[]{};
        List<String> dayList = new ArrayList<String>();
        try {
            Date d1 = com.lamfire.utils.DateUtils.parse(sdate, "yyyyMMdd");
            Date d2 = com.lamfire.utils.DateUtils.parse(edate, "yyyyMMdd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d1);
            while (calendar.getTime().before(d2)) {
                dayList.add(format(calendar));
                calendar.add(Calendar.DATE, 1);
            }
            dayList.add(format(d2));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return dayList.toArray(days);
    }

    /**
     * 计算返回俩日期的周数组
     *
     * @param sdate
     * @param edate
     * @return
     */
    public static String[] dayWeeks(String sdate, String edate) {
        String[] weeks = new String[]{};
        List<String> weekList = new ArrayList<String>();
        int count = 1;
        try {
            Date d1 = com.lamfire.utils.DateUtils.parse(sdate, "yyyyMMdd");
            Date d2 = com.lamfire.utils.DateUtils.parse(edate, "yyyyMMdd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d1);
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY);
            while (calendar.getTime().before(d2)) {
                String monday = format(calendar);
                calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY);
                String sunday = format(calendar);
                weekList.add(monday + "-" + sunday);
                calendar.add(Calendar.DAY_OF_WEEK, 1);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return weekList.toArray(weeks);
    }

    /**
     * 计算返回俩日期的周数组
     *
     * @param sdate
     * @param edate
     * @return
     */
    public static String[] dayMonths(String sdate, String edate) {
        String[] weeks = new String[]{};
        List<String> weekList = new ArrayList<String>();
        int count = 1;
        try {
            Date d1 = com.lamfire.utils.DateUtils.parse(sdate, "yyyyMMdd");
            Date d2 = com.lamfire.utils.DateUtils.parse(edate, "yyyyMMdd");
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(d1);
            calendar.setFirstDayOfWeek(Calendar.MONDAY);
            calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMinimum(Calendar.DAY_OF_MONTH));
            while (calendar.getTime().before(d2)) {
                String firstDay = format(calendar);
                calendar.set(Calendar.DAY_OF_MONTH, calendar.getActualMaximum(Calendar.DAY_OF_MONTH));
                String lastDay = format(calendar);
                weekList.add(firstDay + "-" + lastDay);
                calendar.add(Calendar.DAY_OF_MONTH, 1);
                count++;
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return weekList.toArray(weeks);
    }

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

    public static String getHouryRegx(String startDate, String endDate) {
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
                return startYear + startMonth + "????";
            } else {
                // 月份不同
                return startYear + "??????";
            }
        } else {
            return "*";
        }
    }

    public static void main(String[] arg) {
        String sdate = "20140401";
        String edate = "20140630";
        String[] days = days(sdate, edate);
        for (String day : days) {
            System.out.println(day);
        }
    }
}
