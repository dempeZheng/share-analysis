package cn.sharesdk.analysis.common;

import java.text.SimpleDateFormat;
import java.util.Calendar;

/**
 * Created by dempe on 14-5-28.
 */
public class FrequencyHelper {

    public static int[] startFreq = {1, 3, 6, 10, 20, 50};
    public static int[] pageFreq = {1, 3, 6, 10, 30, 100};
    public static int[] dayUserDurFreq = {0, 3, 10, 30, 60, 180, 600, 1800};
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public static String getStartFrequency(int count) {
        for (int i = 0; i < startFreq.length; i++) {
            if (startFreq[i] > count) {
                return startFreq[i - 1] + "-" + (startFreq[i] - 1);
            }
        }
        return startFreq[startFreq.length - 1] + "+";
    }

    public static String getPageFreq(int count) {
        for (int i = 0; i < pageFreq.length; i++) {
            if (pageFreq[i] > count) {
                return pageFreq[i - 1] + "-" + (pageFreq[i] - 1);
            }
        }
        return startFreq[startFreq.length - 1] + "+";
    }

    public static String getDayUseDurationFreq(Long duration) {
        int length = dayUserDurFreq.length;
        for (int i = 1; i < length; i++) {
            if (duration < dayUserDurFreq[i]) {
                return dayUserDurFreq[i - 1] + 1 + "-" + dayUserDurFreq[i];
            }
        }
        return dayUserDurFreq[length - 1] + "+";
    }

    public static String getDayIntervalFreq(String createDate, String lastEndDate) {
        if (createDate.equals(lastEndDate)) {
            return 0 + "-";
        }
        int day = 0;
        try {
            Calendar calendar1 = Calendar.getInstance();
            calendar1.setTime(simpleDateFormat.parse(createDate));
            int day1 = calendar1.get(Calendar.DAY_OF_YEAR);

            Calendar calendar2 = Calendar.getInstance();
            calendar2.setTime(simpleDateFormat.parse(lastEndDate));
            int day2 = calendar2.get(Calendar.DAY_OF_YEAR);
            day = day1 - day2;
        } catch (Exception e) {

        }

        return getDayInterFreq(day);
    }

    private static String getDayInterFreq(int day) {
        if (day == 0 || day < 0) {
            return "0-24";
        } else if (day > 0 && day < 8) {
            return String.valueOf(day);
        } else if (day < 15) {
            return "8-14";
        } else if (day < 31) {
            return "15-30";
        } else {
            return "30~";
        }


    }


}
