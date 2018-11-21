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
package quickstart;

import com.alibaba.rocketmq.client.consumer.DefaultMQPushConsumer;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import com.alibaba.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import com.alibaba.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.common.consumer.ConsumeFromWhere;
import com.alibaba.rocketmq.common.message.MessageExt;
import io.netty.util.CharsetUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.List;



/**
 * Consumer1，订阅消息
 */
public class Consumer {

    static Logger logger=LoggerFactory.getLogger(Consumer.class);

    public static void main(String[] args) throws InterruptedException, MQClientException {
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer("please_rename_unique_group_name_4");

        consumer.setNamesrvAddr("192.168.232.130:9876;192.168.232.132:9876");
        /**
         * 设置Consumer第一次启动是从队列头部开始消费还是队列尾部开始消费<br>
         * 如果非第一次启动，那么按照上次消费的位置继续消费
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*订阅的主题消息*/
        consumer.subscribe("TopicTest", "*");

        /*如果先启动的是消息生产端的话,这个参数设定对于消费端每次拉取的消息数量是有效果的，拉取的消息最大数量为10
        * 如果先启动的是消费端(先订阅),再启动生产端生产数据,这个参数设定不起效果,，每次拉取到的消息数量实际就是一条数据
        * 正规的操作：先启动消费端，再启动生产端 这样保证消息处理错误进行回滚，再次重新消费这条数据，如果是批量的消息，不容易做到这一点的
        *
        * DefaultMQPushConsumer 推送模式不介意采用设置批处理的方式 pull方式才会采用批量处理的方式
        * 一般就是一次处理一条数据
        * */
        consumer.setConsumeMessageBatchMaxSize(10);


        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            /*注意:一次拉取的消息数量取决于消费端和生产端的启动顺序*/
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {

                System.out.println("消费端读取的数据条数:"+msgs.size());
                MessageExt exceptionMessage=null;
                try {
                    //System.out.println(Thread.currentThread().getName() + " Receive New Messages: " + msgs);
                    for(MessageExt messageExt:msgs){
                        System.out.println("Topic:"+messageExt.getTopic()+" Tag:"+messageExt.getTags()+" Body:"+new String(messageExt.getBody(),CharsetUtil.UTF_8));

                        //模拟消费端处理数据异常,消息重试的处理
                        if(messageExt.getTags().equals(String.valueOf(3))){
                            exceptionMessage=messageExt;
                            throw new Exception("消息处理异常!");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                    //消息重试处理 重试两次之后打印日志
                    if(exceptionMessage!=null && exceptionMessage.getReconsumeTimes()==2){
                        System.out.println("消费端待处理数据："+exceptionMessage.toString()+e.toString());
                        logger.info("消费端待处理数据",exceptionMessage.toString(),e);

                        return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
                    }
                    /*消息重新发送*/
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;

                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        //消费线程启动
        consumer.start();
        System.out.println("Consumer1 Started.");
    }
}
