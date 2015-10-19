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
public class ConsumerHello extends Thread {
    final static String clientId = "SimpleConsumerDemoClient12";
    ConsumerConnector consumerConnector;
    Map<String, Integer> topicCountMap;
    public boolean modeConsumer;
    public String channelName;

    public ConsumerConnector getConsumerConnector() {
        return consumerConnector;
    }
    
    public static void main(String[] argv) throws UnsupportedEncodingException {
        Random rand = new Random();
        String id= Integer.toString((int) rand.nextInt(50) + 1);

        ConsumerHello helloKafkaConsumer = new ConsumerHello(id);
        helloKafkaConsumer.start();
    }

    public ConsumerHello(String id){
        Properties props = new Properties();
        props.put("zookeeper.connect","localhost:2181");
        props.put("group.id",id);
//        props.put("zookeeper.session.timeout.ms", "400");
//        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");
//        props.put("consumer.tcimeout.ms","2000");
        props.put("auto.offset.reset","smallest");
        ConsumerConfig consumerConfig = new ConsumerConfig(props);
        consumerConnector = Consumer.createJavaConsumerConnector(consumerConfig);
    }
    
    private ConsumerIterator<byte[], byte[]> getStream(String topic_) {
        topicCountMap = new HashMap<String, Integer>();
        topicCountMap.put(topic_, new Integer(1));
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumerConnector.createMessageStreams(topicCountMap);
        KafkaStream<byte[], byte[]> stream =  consumerMap.get(topic_).get(0);
        ConsumerIterator<byte[], byte[]> it = stream.iterator();
        return it;
    }

    @Override
    public void run() {
        if (!modeConsumer) {
            ConsumerIterator<byte[], byte[]> it = getStream (TOPIC);
            while(it.hasNext()) {
                String message = new String(it.next().message());
                String tmp = message;
                int idx = getIdxMark (tmp);
                String mode = tmp.substring(0, idx);
                if (mode.equals("NICK")) {
                    String nickname = tmp.substring(idx+1, tmp.length());
                    setNickname(nickname);
                }
            }
        }
        else {
            ConsumerIterator<byte[], byte[]> ci = getStream (channelName);
            while(ci.hasNext()){
                String message = new String(ci.next().message());
                System.out.println(message);
            }
        }
    }
    
    public int getIdxMark (String target) {
        char tmp;
        int i=0;
        boolean stopper = false;
        while ((i<target.length()) && (!stopper)) {
            tmp = target.charAt(i);
            if (tmp == ':')
                stopper = true;
            else
                i++;
        }
        return i;
    }
    
    public static void setNickname(String nickname){
        PAT_IRC_ApacheKafka.listNick.add(nickname);
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