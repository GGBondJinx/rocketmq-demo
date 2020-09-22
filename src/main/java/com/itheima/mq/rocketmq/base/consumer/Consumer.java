package com.itheima.mq.rocketmq.base.consumer;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;

import java.util.List;

/**
 * @author GG Bond
 * @date 2020/9/22 21:19
 * @description 消息的消费者
 */
public class Consumer {

    public static void main(String[] args) throws MQClientException {
        // 1. 创建消费者Consumer，制定消费者组名
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("group1");
        // 2. 指定Nameserver地址
        consumer.setNamesrvAddr("192.168.139.128:9876;102.168.139.129:9876");
        // 3. 订阅主题Topic和Tag
        consumer.subscribe("base1", "Tag1");

        // 设置消费模式：负债均衡 | 广播模式，默认为负债均衡
        consumer.setMessageModel(MessageModel.BROADCASTING);

        // 4. 设置回调函数，处理消息
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            /**
             * 接收消息内容的方法
             * @param list
             * @param consumeConcurrentlyContext
             * @return
             */
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> list, ConsumeConcurrentlyContext consumeConcurrentlyContext) {
                for (MessageExt messageExt : list) {
                    System.out.println(new String(messageExt.getBody()));
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });
        // 5. 启动消费者consumer
        consumer.start();
    }
}
