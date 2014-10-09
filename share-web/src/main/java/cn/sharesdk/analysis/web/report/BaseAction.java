package cn.sharesdk.analysis.web.report;


import cn.sharesdk.analysis.web.Action;

import java.util.List;
import java.util.Map;

/**
 * Created by dempe on 14-6-12.
 */
public abstract class BaseAction implements Action {
    /**
     * 获取参数.
     *
     * @param param
     * @return
     */
    public String getParameter(Map<String, List<String>> params, String param) {
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


}
