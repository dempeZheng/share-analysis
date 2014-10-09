package cn.sharesdk.analysis.web;

import com.lamfire.logger.Logger;
import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.http.HttpRequestDecoder;
import org.jboss.netty.handler.codec.http.HttpResponseEncoder;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class HttpAPIServer {
    private static final Logger LOGGER = Logger.getLogger(HttpAPIServer.class);
    ExecutorService worker;
    ExecutorService boss;
    ServerBootstrap bootstrap;
    private ActionRegistry registry;
    private String hostname;
    private int port;

    public HttpAPIServer(ActionRegistry registry, String hostname, int port) {
        this.registry = registry;
        this.hostname = hostname;
        this.port = port;
    }

    public void startup() {
        this.boss = Executors.newFixedThreadPool(4);
        this.worker = Executors.newFixedThreadPool(32);
        this.bootstrap = new ServerBootstrap(new NioServerSocketChannelFactory(this.boss, this.worker));
        bootstrap.setPipelineFactory(new ServerPipelineFactory());
        bootstrap.setOption("child.tcpNoDelay", true);
        bootstrap.setOption("reuserAddress", true);
        bootstrap.setOption("child.keepAlive", true);
        bootstrap.bind(new InetSocketAddress(hostname, port));
        LOGGER.info("Server start on /" + hostname + ":" + port);

    }

    public void shutdown() {
        this.boss.shutdown();
        this.worker.shutdown();
        this.bootstrap.shutdown();
        System.exit(0);
    }

    private class ServerPipelineFactory implements ChannelPipelineFactory {
        public ChannelPipeline getPipeline() throws Exception {
            ChannelPipeline pipeline = Channels.pipeline();
            pipeline.addLast("decoder", new HttpRequestDecoder());
            pipeline.addLast("encoder", new HttpResponseEncoder());
            pipeline.addLast("handler", new ServerHandler(registry, worker));
            return pipeline;
        }
    }
}
