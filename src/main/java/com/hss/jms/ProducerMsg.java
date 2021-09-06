package com.hss.jms;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;

/**
 * 生产者 -- 用户产生消息
 */
@Component
public class ProducerMsg {
    /**
     * 生成者组
     */
    @Value("${apache.rocketmq.producer.producerGroup}")
    private String producerGroup;
    /**
     * 从配置文件读取 nameserver
     */
    @Value("${apache.rocketmq.namesrvAddr}")
    private String namesrvAddr;

    /**
     * 默认生产者
     */
    private DefaultMQProducer producer;

    /**
     * 提供get方法，让外部调用
     * @return
     */
    public DefaultMQProducer getProducer() {
        return producer;
    }

    @PostConstruct
    public void defaultMQProducer(){
//        1.创建一个默认的生产者对象 -- 作用用于生成消息
        producer = new DefaultMQProducer(producerGroup);
//        2.绑定生产者和nameserver,就是建立和 broker程序的关系
        producer.setNamesrvAddr(namesrvAddr);
//        3.发送消息
        try {
            producer.start();
        } catch (MQClientException e) {
            e.printStackTrace();
        }
    }
}
