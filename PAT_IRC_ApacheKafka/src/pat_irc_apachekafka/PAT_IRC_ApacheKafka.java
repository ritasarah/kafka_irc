/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package pat_irc_apachekafka;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 *
 * @author Andarias Silvanus & Rita Sarah
 */
public class PAT_IRC_ApacheKafka {
    final static String TOPIC = "topic";
    private static String NICKNAME;
    private static String HOST = "localhost";
    public static ArrayList<String> listNick;
    public static ArrayList<String> listChannel;
    
    public static Properties properties;
    
    kafka.javaapi.producer.Producer<String,String> producer;
       
    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        properties = new Properties();
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("auto.create.topics.enable","true");
        
        ProducerConfig producerConfig = new ProducerConfig(properties);
        
        Runnable main_thread = new Runnable() {
                @Override
                public void run() {
                    Scanner input = new Scanner(System.in);
                    String mode = "", channelName = "", msg = "";
                    
                    // Mode list
                    System.out.println("Ketik '/NICK nickname_baru' untuk mengganti nickname Anda");
                    System.out.println("Ketik '/JOIN nama_channel_tujuan' untuk bergabung ke channel tujuan");
                    System.out.println("Ketik '/LEAVE nama_channel' untuk meninggalkan channel tertentu");
                    System.out.println("Ketik apapun untuk mengirim pesan Anda secara broadcast");
                    System.out.println("Kirim pesan ke channel tertentu dengan mengetik @nama_channel dan dilanjutkan dengan pesan Anda");
                    System.out.println("Ketik '/EXIT' untuk keluar dari program\n");

                    System.out.println("producer");
                    listNick = new ArrayList<String>();
                    listChannel = new ArrayList<String>();
                    
                    // Set username
                    generateUname();
                    while (listNick.contains(NICKNAME))
                        generateUname();
                    setNickname("NICK:" + NICKNAME);

                    kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);
                    SimpleDateFormat sdf = new SimpleDateFormat();
                    KeyedMessage<String, String> kmessage =new KeyedMessage<String, String>(TOPIC,"Test message from java program " + sdf.format(new Date()));
                    producer.send(kmessage);
                    
                // Operation
                boolean stopper = false;
                while (!stopper) {
                    mode = input.next();
                    if (mode.equals("/EXIT")) {
                        stopper = true;
                        producer.close();
                    }
                    else if (mode.equals("/NICK")) {
                        String nicknameTMP = input.next();
                        if (listNick.contains(nicknameTMP))
                            System.out.println("Nickname '" + nicknameTMP + "' sudah terdaftar, silahkan coba dengan username lain");
                        else {
                            NICKNAME = nicknameTMP;
                            setNickname("NICK:"+NICKNAME);
                            System.out.println("Ganti nickname berhasil!");
                        }
                    }
                    else if (mode.equals("/JOIN")) {
                        String newChannel = input.next();
                        if (!listChannel.contains(newChannel)) {
                            listChannel.add(newChannel);
                            joinChannel(newChannel);
                            System.out.println("Anda sudah berhasil bergabung di channel '"+newChannel+"'!");
                        }
                        else
                            System.out.println("Anda sudah tergabung di channel '"+newChannel+"'");
                    }
                    else if (mode.equals("/GET")) {
                        System.out.println("Username Anda: " + NICKNAME);
                    }
                    else if (mode.equals("/PRINT")) {
                        if (!listNick.isEmpty()) {
                            System.out.println("isi listNick:");
                            for (int i=0; i<listNick.size(); i++)
                                System.out.println(listNick.get(i));
                        }
                        else
                            System.out.println("listNick kosong");
                    }
                    else if (mode.equals("/LEAVE")) {
                        String channeLeave = input.next();
                        if (listChannel.contains(channeLeave)) {
                            listChannel.remove(channeLeave);
                            leaveChannel(channeLeave);
                            System.out.println("Anda sudah berhasil keluar channel '"+channeLeave+"'!");
                        }
                        else
                            System.out.println("Anda belum tergabung di channel '"+channeLeave+"'");
                    }
                    else{
                        //tambahin cek udah join channel tsb belom kalau mau
                        if (mode.charAt(0) == '@') { // Message channel X
                            channelName = mode.substring(1, mode.length());
                            msg = input.nextLine();
                            String message = "[" + channelName + "]" + " (" + NICKNAME + ") " + msg;
                            sendMessage(channelName, message);
                        }
                        else { // Message to all channel
                            msg = mode + input.nextLine();
                            if (!listChannel.isEmpty()) {
                                for (String channelTmp : listChannel) {
                                    String message = "[" + channelTmp + "]" + " (" + NICKNAME + ") " + msg;
                                    sendMessage(channelTmp, message);
                                }
                            }
                            else
                                System.out.println("Anda belum terdaftar ke channel manapun");
                        }
                    }
                }
                producer.close();
            } 
        };
        new Thread(main_thread).start();
        
        
        Runnable cons = new Runnable() {
                @Override
                public void run() {
                    System.out.println("Consumer");
                    Random rand = new Random();
                    String id= Integer.toString((int) rand.nextInt(50) + 1);
                    
                    ConsumerHello helloKafkaConsumer = new ConsumerHello(id);
//                    ConsumerHello helloKafkaConsumer = new ConsumerHello(NICKNAME);
                    helloKafkaConsumer.start();
                }
        };
        new Thread(cons).start();
        
    }
    
    private static void generateUname(){
	String usernames[] = {"Ludger","Elle","Jude","Milla","Alvin","Rowen","Elize","Leia"};
	String uname;
        Random rand = new Random();
	
	uname = usernames[(int)(rand.nextInt(usernames.length))] + (int) rand.nextInt(50) + 1;
	System.out.println("Username Anda: " + uname + "\n");
	
	NICKNAME = uname;
    }
    
    /*Benerin properties.put nya */
    //Hapus nama lama
    public static void setNickname(String nick){
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("auto.create.topics.enable","true");
        ProducerConfig producerConfig = new ProducerConfig(properties);

        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        KeyedMessage<String, String> kmessage = new KeyedMessage<String, String>(TOPIC,nick);
        producer.send(kmessage);
        producer.close();
    }
    
    public static void joinChannel(String channel){
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("auto.create.topics.enable","true");
        ProducerConfig producerConfig = new ProducerConfig(properties);

        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        KeyedMessage<String, String> kmessage =new KeyedMessage<String, String>("CHANNEL",channel);
        producer.send(kmessage);
        producer.close();
    }
    
    public static void leaveChannel(String channel){
     //ini ngapain ya 
    }
    
    public static void sendMessage(String channel ,String message){
        properties.put("metadata.broker.list","localhost:9092");
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("auto.create.topics.enable","true");
        ProducerConfig producerConfig = new ProducerConfig(properties);

        kafka.javaapi.producer.Producer<String,String> producer = new kafka.javaapi.producer.Producer<String, String>(producerConfig);

        KeyedMessage<String, String> kmessage =new KeyedMessage<String, String>(channel,message);
        producer.send(kmessage);
        producer.close();
    
    }
    
}
