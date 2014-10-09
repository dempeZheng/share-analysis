package cn.sharesdk.analysis.web;


import java.util.List;
import java.util.Map;

public interface Action {

    public byte[] execute(ActionContext context, byte[] message, Map<String, List<String>> params);

}
