package cn.sharesdk.analysis.web;

import org.jboss.netty.handler.codec.http.HttpResponseStatus;

import java.util.List;
import java.util.Map;

public class ActionTask implements Runnable {
    private Action action;
    private ActionContext context;
    private byte[] message;
    private Map<String, List<String>> params;

    public ActionTask(ActionContext context, Action action, byte[] message) {
        this.context = context;
        this.action = action;
        this.message = message;
    }

    public ActionTask(ActionContext context, Action action, byte[] message, Map<String, List<String>> params) {
        this.context = context;
        this.action = action;
        this.message = message;
        this.params = params;
    }

    @Override
    public void run() {
        try {
            byte[] result = action.execute(context, message, params);
            ActionWriter.writeResponse(context.getChannel(), context.getHttpResponse(), result);
        } catch (Throwable t) {
            ActionWriter.writeError(context.getChannel(), HttpResponseStatus.INTERNAL_SERVER_ERROR);
        }
    }

}
