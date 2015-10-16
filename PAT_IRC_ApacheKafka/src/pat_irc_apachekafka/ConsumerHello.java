/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pat_irc_apachekafka;

/**
 *
 * @author Rita Sarah & Andarias Silvanus
 */

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import kafka.consumer.*;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

    import java.util.*;
 
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;
import static pat_irc_apachekafka.PAT_IRC_ApacheKafka.TOPIC;


/**
 * Created by user on 8/4/14.
 */
public class ConsumerHello extends  Thread {
    final static String clientId = "SimpleConsumerDemoClient12";
    ConsumerConnector consumerConnector;


    public static void main(String[] argv) throws UnsupportedEncodingException {
        Random rand = new Random();
        String id= Integer.toString((int) rand.nextInt(50) + 1);
        
        ConsumerHello helloKafkaConsumer = new ConsumerHello(id);
        helloKafkaConsumer.start();
    }

    public ConsumerHello(String id){
        Properties props = new Properties();
        props.put("zookeeper.connect","localhost:2181");
        props.put("group.id","test-group-"+id);
//        props.put("zookeeper.session.timeout.ms", "400");
//        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("consumer.timeout.ms","2000");
        props.put("auto.offset.reset","smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }

    @Override
    public void run() {
        Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(TOPIC, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(TOPIC).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        System.out.println("waiting for messages...");
        while(it.hasNext()){
            System.out.println(new String(it.next().message()));
        }

    }
    
    public static void setNickname(){
    
    }

    private static void printMessages(ByteBufferMessageSet messageSet) throws UnsupportedEncodingException {
        for(MessageAndOffset messageAndOffset: messageSet) {
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes = new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(new String(bytes, "UTF-8"));
        }
    }
    
}