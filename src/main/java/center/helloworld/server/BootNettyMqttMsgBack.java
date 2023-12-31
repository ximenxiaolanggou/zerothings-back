package center.helloworld.server;

import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.handler.codec.mqtt.*;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttPubRec;
import org.eclipse.paho.client.mqttv3.internal.wire.MqttReceivedMessage;
import org.springframework.boot.jackson.JsonComponentModule;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author zhishun.cai
 * @create 2023/4/11
 * @note
 */
@Slf4j
public class BootNettyMqttMsgBack {

    private static final Map<String, Set<Channel>> subscriptions = new ConcurrentHashMap<>();

    private static final Map<Integer, MqttPublishMessage> messagesQos2 = new ConcurrentHashMap<>();

    /**
     * 	确认连接请求
     * @param channel
     * @param mqttMessage
     */
    public static void connack (Channel channel, MqttMessage mqttMessage) {
        MqttConnectMessage mqttConnectMessage = (MqttConnectMessage) mqttMessage;
        MqttFixedHeader mqttFixedHeaderInfo = mqttConnectMessage.fixedHeader();
        MqttConnectVariableHeader mqttConnectVariableHeaderInfo = mqttConnectMessage.variableHeader();

        //	构建返回报文， 可变报头
        MqttConnAckVariableHeader mqttConnAckVariableHeaderBack = new MqttConnAckVariableHeader(MqttConnectReturnCode.CONNECTION_ACCEPTED, mqttConnectVariableHeaderInfo.isCleanSession());
        //	构建返回报文， 固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.CONNACK,mqttFixedHeaderInfo.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
        //	构建CONNACK消息体
        MqttConnAckMessage connAck = new MqttConnAckMessage(mqttFixedHeaderBack, mqttConnAckVariableHeaderBack);
        log.info("back--"+connAck.toString());
        channel.writeAndFlush(connAck);
    }

    /**
     * 	根据qos发布确认
     * @param channel
     * @param mqttMessage
     */
    public static void puback (Channel channel, MqttMessage mqttMessage) {
        MqttPublishMessage mqttPublishMessage = (MqttPublishMessage) mqttMessage;
        String topicName = mqttPublishMessage.variableHeader().topicName();
        MqttFixedHeader mqttFixedHeaderInfo = mqttPublishMessage.fixedHeader();
        MqttPublishVariableHeader mqttPublishVariableHeader = mqttPublishMessage.variableHeader();
        MqttQoS qos = (MqttQoS) mqttFixedHeaderInfo.qosLevel();
//        byte[] headBytes = new byte[mqttPublishMessage.payload().readableBytes()];
//        mqttPublishMessage.payload().readBytes(headBytes);
//        String data = new String(headBytes);
//        System.out.println("publish data--"+data);

        switch (qos) {
            case AT_MOST_ONCE: 		//	至多一次
                break;
            case AT_LEAST_ONCE:		//	至少一次
                Set<Channel> subscribers = subscriptions.get(topicName);
                if (subscribers != null) {
                    for (Channel subscriber : subscribers) {
                        subscriber.writeAndFlush(mqttPublishMessage);
                    }
                }
                //	构建返回报文， 可变报头
                MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                //	构建返回报文， 固定报头
                MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBACK,mqttFixedHeaderInfo.isDup(), MqttQoS.AT_MOST_ONCE, mqttFixedHeaderInfo.isRetain(), 0x02);
                //	构建PUBACK消息体
                MqttPubAckMessage pubAck = new MqttPubAckMessage(mqttFixedHeaderBack, mqttMessageIdVariableHeaderBack);
                log.info("back--"+pubAck.toString());
                channel.writeAndFlush(pubAck);
                break;
            case EXACTLY_ONCE:		//	刚好一次
                //	构建返回报文， 固定报头
                MqttFixedHeader mqttFixedHeaderBack2 = new MqttFixedHeader(MqttMessageType.PUBREC,false, MqttQoS.AT_LEAST_ONCE,false,0x02);
                //	构建返回报文， 可变报头
                MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack2 = MqttMessageIdVariableHeader.from(mqttPublishMessage.variableHeader().packetId());
                MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack2,mqttMessageIdVariableHeaderBack2);
                log.info("back--"+mqttMessageBack.toString());
                channel.writeAndFlush(mqttMessageBack);
                messagesQos2.put(mqttPublishVariableHeader.messageId(), mqttPublishMessage);
                break;
            default:
                break;
        }

    }

    /**
     * 	发布完成 qos2
     * @param channel
     * @param mqttMessage
     */
    public static void pubcomp (Channel channel, MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();

        int messageId = messageIdVariableHeader.messageId();
        MqttPublishMessage mqttPublishMessage = messagesQos2.get(messageId);
        String topicName = mqttPublishMessage.variableHeader().topicName();
        Set<Channel> subscribers = subscriptions.get(topicName);
        if (subscribers != null) {
            for (Channel subscriber : subscribers) {
                subscriber.writeAndFlush(mqttPublishMessage);
            }
        }

        //	构建返回报文， 固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBCOMP,false, MqttQoS.AT_MOST_ONCE,false,0x02);
        //	构建返回报文， 可变报头
        MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack,mqttMessageIdVariableHeaderBack);
        log.info("back--"+mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }

    /**
     * 	订阅确认
     * @param channel
     * @param mqttMessage
     */
    public static void suback(Channel channel, MqttMessage mqttMessage) {
        MqttSubscribeMessage mqttSubscribeMessage = (MqttSubscribeMessage) mqttMessage;
        MqttMessageIdVariableHeader messageIdVariableHeader = mqttSubscribeMessage.variableHeader();
        //	构建返回报文， 可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        Set<String> topics = mqttSubscribeMessage.payload().topicSubscriptions().stream().map(mqttTopicSubscription -> mqttTopicSubscription.topicName()).collect(Collectors.toSet());

        // 维护topic 和 通道关系
        for (String topic : topics) {
            // 订阅主题
            Set<Channel> subscribers = subscriptions.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet());
            subscribers.add(channel);
        }

        List<Integer> grantedQoSLevels = new ArrayList<>(topics.size());
        for (int i = 0; i < topics.size(); i++) {
            grantedQoSLevels.add(mqttSubscribeMessage.payload().topicSubscriptions().get(i).qualityOfService().value());
        }
        //	构建返回报文	有效负载
        MqttSubAckPayload payloadBack = new MqttSubAckPayload(grantedQoSLevels);
        //	构建返回报文	固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.SUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2+topics.size());
        //	构建返回报文	订阅确认
        MqttSubAckMessage subAck = new MqttSubAckMessage(mqttFixedHeaderBack,variableHeaderBack, payloadBack);
        log.info("back--"+subAck.toString());
        channel.writeAndFlush(subAck);
    }

    /**
     * 	取消订阅确认
     * @param channel
     * @param mqttMessage
     */
    public static void unsuback(Channel channel, MqttMessage mqttMessage) {
        MqttUnsubscribeMessage mqttUnsubscribeMessage = (MqttUnsubscribeMessage)mqttMessage;
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();
        List<String> topics = mqttUnsubscribeMessage.payload().topics();

        for (String topic : topics) {
            Set<Channel> subscribers = subscriptions.get(topic);
            if (subscribers != null) {
                subscribers.remove(channel);
                if (subscribers.isEmpty()) {
                    subscriptions.remove(topic);
                }
            }
        }

        //	构建返回报文	可变报头
        MqttMessageIdVariableHeader variableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        //	构建返回报文	固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.UNSUBACK, false, MqttQoS.AT_MOST_ONCE, false, 2);
        //	构建返回报文	取消订阅确认
        MqttUnsubAckMessage unSubAck = new MqttUnsubAckMessage(mqttFixedHeaderBack,variableHeaderBack);
        log.info("back--"+unSubAck.toString());
        channel.writeAndFlush(unSubAck);
    }

    /**
     * 	心跳响应
     * @param channel
     * @param mqttMessage
     */
    public static void pingresp (Channel channel, MqttMessage mqttMessage) {
        //	心跳响应报文	11010000 00000000  固定报文
        MqttFixedHeader fixedHeader = new MqttFixedHeader(MqttMessageType.PINGRESP, false, MqttQoS.AT_MOST_ONCE, false, 0);
        MqttMessage mqttMessageBack = new MqttMessage(fixedHeader);
        log.info("back--"+mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }


    public static void pubrel(Channel channel, MqttMessage mqttMessage) {
        MqttMessageIdVariableHeader messageIdVariableHeader = (MqttMessageIdVariableHeader) mqttMessage.variableHeader();

        //	构建返回报文， 固定报头
        MqttFixedHeader mqttFixedHeaderBack = new MqttFixedHeader(MqttMessageType.PUBREL,false, MqttQoS.AT_MOST_ONCE,false,0x02);
        //	构建返回报文， 可变报头
        MqttMessageIdVariableHeader mqttMessageIdVariableHeaderBack = MqttMessageIdVariableHeader.from(messageIdVariableHeader.messageId());
        MqttMessage mqttMessageBack = new MqttMessage(mqttFixedHeaderBack,mqttMessageIdVariableHeaderBack);
        log.info("back--"+mqttMessageBack.toString());
        channel.writeAndFlush(mqttMessageBack);
    }
}
