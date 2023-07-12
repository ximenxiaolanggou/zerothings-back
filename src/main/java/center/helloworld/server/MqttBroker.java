package center.helloworld.server;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.mqtt.MqttDecoder;
import io.netty.handler.codec.mqtt.MqttEncoder;
import io.netty.handler.codec.mqtt.MqttMessage;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.netty.handler.timeout.IdleStateHandler;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author zhishun.cai
 * @create 2023/4/11
 * @note
 */
public class MqttBroker {
    private NioEventLoopGroup bossGroup;

    private NioEventLoopGroup workGroup;

    public static void main(String[] args) {
        MqttBroker bootNettyMqttServer = new MqttBroker();
        bootNettyMqttServer.startup(1883);
    }

    /**
     * 	启动服务
     */
    public void startup(int port) {

        try {
            bossGroup = new NioEventLoopGroup(1);
            workGroup = new NioEventLoopGroup();

            ServerBootstrap bootstrap = new ServerBootstrap();
            bootstrap.group(bossGroup, workGroup);
            bootstrap.channel(NioServerSocketChannel.class);

            bootstrap.option(ChannelOption.SO_REUSEADDR, true)
                    .option(ChannelOption.SO_BACKLOG, 1024)
                    .option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT)
                    .option(ChannelOption.SO_RCVBUF, 10485760);

            bootstrap.childOption(ChannelOption.TCP_NODELAY, true)
                    .childOption(ChannelOption.SO_KEEPALIVE, true)
                    .childOption(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);

            bootstrap.childHandler(new ChannelInitializer<SocketChannel>() {
                protected void initChannel(SocketChannel ch) {
                    ChannelPipeline channelPipeline = ch.pipeline();
                    // 设置读写空闲超时时间
                    channelPipeline.addLast(new IdleStateHandler(10, 600, 1200));
                    channelPipeline.addLast("encoder", MqttEncoder.INSTANCE);
                    channelPipeline.addLast("decoder", new MqttDecoder());
                    ch.pipeline().addLast(new ChannelDuplexHandler() {
                        @Override
                        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
                            IdleStateEvent event = (IdleStateEvent) evt;
                            // 读事件
                            if(event.state() == IdleState.READER_IDLE) {
                                System.out.println("检测到10s未接收到数据~~");
                                ctx.channel().close();
                            }
                        }
                    });
                    channelPipeline.addLast(new BootNettyMqttChannelInboundHandler());
                }
            });
            ChannelFuture f = bootstrap.bind(port).sync();
            if(f.isSuccess()){
                System.out.println("startup success port = " + port);
                f.channel().closeFuture().sync();
            } else {
                System.out.println("startup fail port = " + port);
            }


//			//	绑定端口,监听服务是否启动
//			bootstrap.bind(port).addListener((ChannelFutureListener) channelFuture -> {
//				if (channelFuture.isSuccess()) {
//					System.out.println("startup success --- ");
//				} else {
//					System.out.println("startup fail ---");
//				}
//			});
        } catch (Exception e) {
            System.out.println("start exception"+e.toString());
        }

    }

    /**
     * 	关闭服务
     */
    public void shutdown() throws InterruptedException {
        if (workGroup != null && bossGroup != null) {
            bossGroup.shutdownGracefully().sync();
            workGroup.shutdownGracefully().sync();
            System.out.println("shutdown success");
        }
    }

}

class MqttServer extends SimpleChannelInboundHandler<MqttMessage> {
    private final Map<String, Set<Channel>> subscriptions = new ConcurrentHashMap<>();

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, MqttMessage msg) throws Exception {
        System.out.println("channelRead0");
        // 处理MQTT消息
        // ...
    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) throws Exception {
        // 客户端连接断开时，取消订阅并清除缓存
        System.out.println("channelInactive");
        // ...
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ctx.fireChannelRead(msg);
    }

    // 订阅主题
    private void subscribe(Channel channel, String topic) {
        Set<Channel> subscribers = subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet());
        subscribers.add(channel);
    }

    // 取消订阅
    private void unsubscribe(Channel channel, String topic) {
        Set<Channel> subscribers = subscriptions.get(topic);
        if (subscribers != null) {
            subscribers.remove(channel);
            if (subscribers.isEmpty()) {
                subscriptions.remove(topic);
            }
        }
    }

    // 发布消息
    private void publish(String topic, MqttMessage message) {
        Set<Channel> subscribers = subscriptions.get(topic);
        if (subscribers != null) {
            for (Channel subscriber : subscribers) {
                subscriber.writeAndFlush(message);
            }
        }
    }
}

