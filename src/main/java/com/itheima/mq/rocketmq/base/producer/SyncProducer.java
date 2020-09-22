package com.itheima.mq.rocketmq.base.producer;

import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.SendStatus;
import org.apache.rocketmq.common.message.Message;

import java.util.concurrent.TimeUnit;

/**
 * @author GG Bond
 * @date 2020/9/21 21:58
 * @description 发送同步消息
 */
public class SyncProducer {

    public static void main(String[] args) throws Exception {
        // 1. 创建消息生产者 producer，并制定生产者组名
        DefaultMQProducer producer = new DefaultMQProducer("group1");
        // 2. 指定 Nameserver 地址
        producer.setNamesrvAddr("192.168.139.128:9876;102.168.139.129:9876");
        // 3. 启动 producer
        producer.start();
        for (int i = 0; i < 10; i++) {
            // 4. 创建消息对象，指定主题 Topic、Tag 和消息体
            // 参数一：消息主题 Topic
            // 参数二：消息 Tag
            // 参数三：消息内容
            Message message = new Message("base1", "Tag1", ("Hello World" + (i + 1)).getBytes());
            // 5. 发送消息
            SendResult result = producer.send(message);
            // 发送状态
            SendStatus status = result.getSendStatus();
            // 消息 ID
            String msgId = result.getMsgId();
            // 消息接收队列 ID
            int queueId = result.getMessageQueue().getQueueId();
            System.out.println("发送状态" + result + "消息 ID " + msgId + "，队列 ID " + queueId);

            TimeUnit.SECONDS.sleep(1);
        }
        // 6. 关闭生产者 producer
        producer.shutdown();
    }
}
