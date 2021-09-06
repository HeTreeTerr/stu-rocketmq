package com.hss.jms;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 消息的消费者
 */
@Component
public class ConsumerMsg {
    /**
     * 读取配置文件的 消费者
     */
    @Value("${apache.rocketmq.consumer.PushConsumer}")
    private String consumerGroup;

    /**
     * 从配置文件读取 nameserver
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    /**
     * 提供一个空构造器
     */
    public ConsumerMsg(){

    }

    /**
     * 提供默认的消费者
     */
    @PostConstruct
    public void defaultMqConsumer() throws MQClientException {
//        1.指定消费者 所消费的主题（队列）和tag（2及标签，用于过滤消息）
//        从消费者拿到1个消费者
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerGroup);

        consumer.setNamesrvAddr(namesrvAddr);
//        说明：如果要消费所有 tag，用统配符*代替所有的tag
        consumer.subscribe("orderTopic","*");
//        2.指定消费者的策略（从所有的消息开头位置执行，还是从消息尾部执行）指定消费的孙旭
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);
//        注册消息监听器，用的lambda
        consumer.registerMessageListener((MessageListenerConcurrently)(list,context)->{
            try{
                for(MessageExt messageExt : list){
//                    输出消息内容
                    System.out.println("messageExt:" + messageExt);
//                    读取远程配置的编码为utf-8
                    String messageBody = new String(messageExt.getBody(), RemotingHelper.DEFAULT_CHARSET);
//                    输出消息内容
                    System.out.println("消费响应: msgId:" + messageExt.getMsgId() + ",msgBody:" + messageBody);
                }
            }catch (Exception e){
                e.printStackTrace();
                //稍后再试
                return ConsumeConcurrentlyStatus.RECONSUME_LATER;
            }
            //消费成功
            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        });
//        3.开启建监听，消费消息
        consumer.start();
    }
}
