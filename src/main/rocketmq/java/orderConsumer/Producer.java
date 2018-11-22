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

import com.alibaba.rocketmq.client.exception.MQBrokerException;
import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.MQProducer;
import com.alibaba.rocketmq.client.producer.MessageQueueSelector;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;
import com.alibaba.rocketmq.common.message.MessageQueue;
import com.alibaba.rocketmq.remoting.exception.RemotingException;

import java.util.List;


/**
 * Producer，发送顺序消息
 */
public class Producer {
    public static void main(String[] args) {
        try {
            MQProducer producer = new DefaultMQProducer("orderGroup");

            ((DefaultMQProducer) producer).setNamesrvAddr("192.168.232.130:9876;192.168.232.132:9876");

            producer.start();

            String[] tags = new String[] { "TagA", "TagB", "TagC", "TagD", "TagE" };

            /*订单A*/
            for (int i = 1; i < 5; i++) {
                // 订单ID相同的消息要有序,一个相同的订单里面有5个子任务 Tag进行区分
                Message msg =
                        new Message("orderTopic", tags[i-1], "KEY" + i,
                            ("订单A :" + i+"任务").getBytes());


                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    /*一个Topic,Mq会默认给创建4个Queue*/
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                       /* Integer id = (Integer) arg;
                        int index = id % mqs.size();*/

                       //相同订单的任务消息放在同一个Queue保证消息的顺序消费,同时不同订单的消息任务数据分布在不同的Queue中进行处理还提高了数据处理的吞吐量
                        return mqs.get(0);
                    }
                }, 0);

                System.out.println("订单A:"+sendResult);
            }

            /*订单B*/
            for (int i = 1; i < 5; i++) {
                // 订单ID相同的消息要有序,一个相同的订单里面有5个子任务 Tag进行区分
                Message msg =
                        new Message("orderTopic", tags[i-1], "KEY" + i,
                                ("订单B :" + i+"任务").getBytes());


                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    /*一个Topic,Mq会默认给创建4个Queue*/
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                       /* Integer id = (Integer) arg;
                        int index = id % mqs.size();*/
                        return mqs.get(1);
                    }
                }, 1);

                System.out.println("订单B:"+sendResult);
            }

            /*订单C*/
            for (int i = 1; i < 5; i++) {
                // 订单ID相同的消息要有序,一个相同的订单里面有5个子任务 Tag进行区分
                Message msg =
                        new Message("orderTopic", tags[i-1], "KEY" + i,
                                ("订单C :" + i+"任务").getBytes());


                SendResult sendResult = producer.send(msg, new MessageQueueSelector() {
                    /*一个Topic,Mq会默认给创建4个Queue*/
                    @Override
                    public MessageQueue select(List<MessageQueue> mqs, Message msg, Object arg) {
                       /* Integer id = (Integer) arg;
                        int index = id % mqs.size();*/
                        return mqs.get(2);
                    }
                }, 2);

                System.out.println("订单C:"+sendResult);
            }

            producer.shutdown();
        }
        catch (MQClientException e) {
            e.printStackTrace();
        }
        catch (RemotingException e) {
            e.printStackTrace();
        }
        catch (MQBrokerException e) {
            e.printStackTrace();
        }
        catch (InterruptedException e) {
            e.printStackTrace();
        }


    }


}
