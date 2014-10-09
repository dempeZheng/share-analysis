package cn.sharesdk.analysis.web;

import com.lamfire.json.JSON;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFutureListener;
import org.jboss.netty.handler.codec.http.DefaultHttpResponse;
import org.jboss.netty.handler.codec.http.HttpHeaders.Names;
import org.jboss.netty.handler.codec.http.HttpResponse;
import org.jboss.netty.handler.codec.http.HttpResponseStatus;
import org.jboss.netty.handler.codec.http.HttpVersion;
import org.jboss.netty.util.CharsetUtil;

class ActionWriter {

    public static void writeResponse(Channel channel, HttpResponse response, byte[] responseMessage) {
        try {
            if (!channel.isWritable()) {
                return;
            }
            int length = 0;
            if (responseMessage != null) {
                ChannelBuffer buffer = ChannelBuffers.wrappedBuffer(responseMessage);
                response.setContent(buffer);
                length = response.getContent().writerIndex();
            }
            response.setHeader("Content-Type", "text/html; charset=UTF-8");
            response.setHeader("Content-Length", String.valueOf(length));
            channel.write(response).addListener(ChannelFutureListener.CLOSE);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }

    public static void writeError(Channel channel, HttpResponseStatus status) {
        try {
            if (!channel.isConnected() || !channel.isWritable()) {
                return;
            }
            HttpResponse response = new DefaultHttpResponse(HttpVersion.HTTP_1_1, status);
            response.setHeader(Names.CONTENT_TYPE, "text/plain; charset=UTF-8");
            JSON json = new JSON();
            json.put("status", status.getCode());
            json.put("message", status.getReasonPhrase());
            response.setContent(ChannelBuffers.copiedBuffer(json.toJSONString(), CharsetUtil.UTF_8));
            channel.write(response).addListener(ChannelFutureListener.CLOSE);
        } catch (Throwable t) {
            t.printStackTrace();
        }
    }
}
