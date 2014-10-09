package cn.sharesdk.analysis.web;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.handler.codec.http.*;

import java.net.InetSocketAddress;
import java.util.Set;

public class ActionContext {
    private HttpRequest request;
    private HttpResponse response;

    private ChannelHandlerContext ctx;

    ActionContext(ChannelHandlerContext ctx, HttpRequest request) {
        this.request = request;
        this.ctx = ctx;
        this.response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, HttpResponseStatus.OK);
    }

    Channel getChannel() {
        return ctx.getChannel();
    }

    public String getRemoteAddress() {
        InetSocketAddress addr = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
        String ip = addr.getAddress().getHostAddress();
        return ip;
    }

    private String findAddress(String forwardFor) {
        if (forwardFor == null || forwardFor.length() == 0) {
            return null;
        }
        String[] addresses = forwardFor.split(",");

        for (String addr : addresses) {
            if (!"unknown".equalsIgnoreCase(addr)) {
                return addr;
            }
        }

        return null;
    }


    public String getRealRemoteAddr() {
        String addr;
        String forwardedFor = request.getHeader("X-Forwarded-For");
        if ((addr = findAddress(forwardedFor)) != null) {
            return addr.trim();
        }

        forwardedFor = request.getHeader("Proxy-Client-IP");
        ;
        if ((addr = findAddress(forwardedFor)) != null) {
            return addr.trim();
        }

        forwardedFor = request.getHeader("WL-Proxy-Client-IP");
        if ((addr = findAddress(forwardedFor)) != null) {
            return addr.trim();
        }
        return getRemoteAddress();
    }

    public int getRemotePort() {
        InetSocketAddress addr = (InetSocketAddress) ctx.getChannel().getRemoteAddress();
        return addr.getPort();
    }

    public String getHttpRequestHeader(String key) {
        return request.getHeader(key);
    }

    public Set<String> getHttpRequestHeaderNames() {
        return request.getHeaderNames();
    }

    public String getHttpRequestUri() {
        return request.getUri();
    }

    public void addHttpResponseHeader(String key, Object value) {
        this.response.addHeader(key, value);
    }

    public void setHttpResponseStatus(HttpResponseStatus status) {
        this.response.setStatus(status);
    }

    public HttpRequest getHttpRequest() {
        return request;
    }

    public HttpResponse getHttpResponse() {
        return response;
    }


}
