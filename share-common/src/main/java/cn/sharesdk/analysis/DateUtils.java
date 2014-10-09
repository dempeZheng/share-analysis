package cn.sharesdk.analysis;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.SimpleFormatter;

/**
 * Created by dempe on 14-6-23.
 */
public class DateUtils {

    public static SimpleDateFormat sdf  = new SimpleDateFormat("yyyyMMdd");
    public static SimpleDateFormat formatter  = new SimpleDateFormat("yyyyMMdd");

    public static String getDateStr(){
        return formatter.format(new Date());
    }
}
