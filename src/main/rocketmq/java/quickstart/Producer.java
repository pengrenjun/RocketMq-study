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


import com.alibaba.rocketmq.client.exception.MQClientException;
import com.alibaba.rocketmq.client.producer.DefaultMQProducer;
import com.alibaba.rocketmq.client.producer.SendResult;
import com.alibaba.rocketmq.common.message.Message;

/**
 * Producer，发送消息
 *
 * linux服务器上使用的是RocketMq 3.2.6版本 因此client common remoting 需要使用相应的版本
 * rocketmq java 客户端调用No route info of this topic错误(原因版本不一致)
 */
public class Producer {
    public static void main(String[] args) throws MQClientException, InterruptedException {
        DefaultMQProducer producer = new DefaultMQProducer("quickstartTopic3");

        producer.setNamesrvAddr("192.168.232.130:9876;192.168.232.132:9876");
       // producer.setVipChannelEnabled(false);
       // producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //设置生产端的消息发送重试次数
        producer.setRetryTimesWhenSendFailed(2);
        producer.start();

        for (int i = 0; i < 10; i++) {
            try {
                Message msg = new Message("TopicTest",// topic
                    String.valueOf(i),// tag
                    ("Hello RocketMQ " + i).getBytes()// body
                        );
                //一秒之内没有发送消息过去,就重试发送
                SendResult sendResult = producer.send(msg,1000);
                System.out.println(sendResult);
            }
            catch (Exception e) {
                e.printStackTrace();
                Thread.sleep(1000);
            }
        }

        producer.shutdown();
    }
}
