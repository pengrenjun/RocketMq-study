/**
 * Copyright (C) 2010-2013 Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package orderConsumer;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeOrderlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerOrderly;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import io.netty.util.CharsetUtil;

import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;


/**
 * 顺序消息消费，带事务方式（应用可控制Offset什么时候提交）
 *
 * Broker 只是保证的是消息按照顺序存放在队列里面 然后按照存入的顺序发送给消费端
 * 要实现顺序的消费 需要实现MessageListenerOrderly接口  实现一个线程从一个Queue里面读取消息 保证消费消息的原子性
 */
public class Consumer1 {

    public static void main(String[] args) throws MQClientException {
        DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("orderGroup");

        consumer.setNamesrvAddr("192.168.232.130:9876;192.168.232.132:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*设置消费端处理消息的线程数量 类似于线程池*/
        consumer.setConsumeThreadMin(10);

        consumer.setConsumeThreadMax(20);

        consumer.subscribe("orderTopic", "*"  /*"TagA || TagC || TagD"*/);


        /*A MessageListenerConcurrently object is used to receive asynchronously delivered messages orderly.one queue,one thread*/
        consumer.registerMessageListener(new MessageListenerOrderly() {
            //AtomicLong consumeTimes = new AtomicLong(0);

            //MessageListenerOrderly使用一个线程从一个Queue里面读取消息数据 为了实现顺序消费 这个实现接口里面不允许在开启多个线程进行消息的处理,要保持消息处理的原子性

            @Override
            public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {


                System.out.println("消费端读取的数据条数:"+msgs.size());
                MessageExt exceptionMessage=null;
                try {
                    //System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                    for(MessageExt messageExt:msgs){
                        System.out.println("Topic:"+messageExt.getTopic()+" Tag:"+messageExt.getTags()+" Body:"+new String(messageExt.getBody(),CharsetUtil.UTF_8));

                       /* //模拟消费端处理数据异常,消息重试的处理
                        if(messageExt.getTags().equals(String.valueOf(3))){
                            exceptionMessage=messageExt;
                            throw new Exception("消息处理异常!");
                        }*/
                        exceptionMessage=messageExt;
                       //模拟消费处理
                        TimeUnit.SECONDS.sleep(new Random().nextInt(2));
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //消息重试处理 重试两次之后打印日志
                    if(exceptionMessage!=null && exceptionMessage.getReconsumeTimes()==2){
                        System.out.println("消费端待处理数据："+exceptionMessage.toString()+e.toString());
                        //logger.info("消费端待处理数据",exceptionMessage.toString(),e);

                        return ConsumeOrderlyStatus.SUCCESS;
                    }
                    /*消息重新发送*/
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;

                }
                return ConsumeOrderlyStatus.SUCCESS;
            }
        });

        consumer.start();

        System.out.println("Consumer1 Started.");
    }

}
