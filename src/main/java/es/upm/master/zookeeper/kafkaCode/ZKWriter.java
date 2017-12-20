package es.upm.master.zookeeper.kafkaCode;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.zookeeper.ZooKeeper;

public class ZKWriter implements Watcher{
    private Stat stat;
    public ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";
    private String online = "/System/Online/";
    private List<String> sendermess = new ArrayList<String>();
    private String kafkaIpPortName;

   // private Map<String, List<String>> sendermess = new HashMap<String, List<String>>();
    public String name;
    public static kafkaUConsole userConsole;


    interface Control {
        byte[] NEW = "-1".getBytes();
        byte[] FAILED = "0".getBytes();
        byte[] SUCCES = "1".getBytes();
        byte[] EXISTS = "2".getBytes();
    }

    public void ZKWriter(String user, kafkaUConsole userC) throws KeeperException, InterruptedException, IOException {
        this.zoo = zooConnect();    // Connects to ZooKeeper service
        this.name=user;
        userConsole=userC;

    }

    public void zooDisconnect() throws InterruptedException, KeeperException {
        quit();
        //Thread.sleep(500);
        zoo.close();
    };


    public void create() throws KeeperException, InterruptedException {

        String path = enroll + name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children

        //first we check if node exists

        if (zoo.exists(path, false) != null) {
            //if exists
            System.out.println("User already registered" + name);
        } else {

            System.out.println("User not registered, proceeding to enroll" + name);
//            System.out.println("this is the path" + path);
            //creates the first node
            try {
                zoo.create(path, ZKWriter.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //Setting watcher if state  of Control changes
                zoo.exists(path , (Watcher) this);
                zoo.getChildren("/System/Online",this);
            } catch (KeeperException.NodeExistsException e) {
                //node existis, changing status
                //zoo.create(path,Test.Control.EXISTS , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); ??
                System.out.println("request to create user"+ name + "already processed. ");
            } catch (InterruptedException e) { }

        }
    }


    public void quit() throws KeeperException, InterruptedException {
        String path = quit + name;
        String pathreg= registry+ name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children
        if(zoo.exists(pathreg, false) != null){
            if (zoo.exists(path, false) == null) {
                System.out.println("Procesding to quit- creating node under quit");
                //create the node who wants to quit the system
                zoo.create(path, ZKWriter.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //Setting watcher if state  of Control changes
                zoo.exists(path , (Watcher) this);

            }else{
                System.out.println("Cant quit, already quitting" + path);
            }
        }else{
            System.out.println("Cant quit a user not registered" + pathreg);
        }
    }

    public void goOnline() throws KeeperException, InterruptedException {
        String path= online + name;
        //check if user is already online
        if (zoo.exists(path,false)!=null){
            System.out.println("user"+name+" already online, not connecting twice");

        }else{
            //create ephemeral node
            System.out.println("USER "+name+" NOT ONLINE >> CONNECTING");
            zoo.create(path, ZKWriter.Control.NEW, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        }

    }

    public void goOffline() throws KeeperException, InterruptedException {
        String path= online + name;
        //check if user is already online
        if (zoo.exists(path, false)!=null){
            System.out.println("user "+name+"disconnecting from online");
            zoo.delete(path,-1);

        }else {
            //create ephemeral node
            System.out.println("USER NOT ONLINE !! SO CAN'T GO OFFLINE");
        }
    }


    public void send(String receiver, String msg) throws KeeperException, InterruptedException, UnsupportedEncodingException {

        Stat statReceiver= zoo.exists(online+name, false);
        //creamos nodo ephemeral sequential under the receiver. but first check if he is connected
            if(statReceiver!=null) {
                //System.out.println("Receiver" + receiver+ " is ONLINE, so we will send the message");
                //zoo.create(queue + receiver, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


                Properties props = new Properties();
                //props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,kafkaIpPortName);
                props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
                props.put("acks", "all");
                props.put("retries", 0);
                props.put("batch.size", 16384);
                props.put("buffer.memory", 33554432);
                props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
                props.put("value.serializer",
                        "org.apache.kafka.common.serialization.StringSerializer");

                KafkaProducer<String, String> prod = new KafkaProducer<String, String>(props);
                String topic = receiver.toString();
                int partition = 0;

                Date date = new Date();
                Timestamp newTime = new Timestamp(date.getTime());
                String key = "{"+ newTime +"} "+ " From: " + name.toString();

                String value = msg.toString();
                prod.send(new ProducerRecord<String, String>(topic,partition,key, value));
                prod.close();
                System.out.println("Sender"+ name + "to receiver : "+ topic + value);

            }else {
            System.out.println("SENDER NOT ONLINE");
        }}

    //ConsumerRecords<String, String>
    public ConsumerRecords<String, String> read() throws KeeperException, InterruptedException, UnsupportedEncodingException {
        List<String> sendermess = new ArrayList<String>();
        //first check if sender is online...
        stat= zoo.exists(online+name, false);
        if (stat!=null){
            //System.out.println("This guy is online and wants to read messages: " + name );
            System.out.println("this guy wants to read  "+ name);


            Properties props = new Properties();
            //props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaIpPortName);
            props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
            props.put("group.id", "MYGROUP");
            props.put("enable.auto.commit", "true");props.put("auto.commit.interval.ms",
                    "1000");props.put("key.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            props.put("value.deserializer",
                    "org.apache.kafka.common.serialization.StringDeserializer");
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
            ConsumerRecords<String, String> records;
            String messageContent = null;
            try{ consumer.subscribe(Arrays.asList("master2016-replicated-java",name));
                while (true) {
                    records = consumer.poll(200);
                    System.out.println("This are the records " + records );

                    for (ConsumerRecord<String, String> record : records){

                        System.out.println("This are the records " + records );

                        System.out.print("Topic: " + record.topic() + ", ");
                        System.out.print("Partition: " + record.partition() + ", ");
                        System.out.print("Key: " + record.key() + ", ");
                        System.out.println("Value: " + record.value() + ", ");


                        messageContent = record.key() + " : " + record.value();
                        System.out.println(messageContent);

                        sendermess.add(messageContent);
                    }
                    userConsole.addMessage(sendermess);
                    return records;

                }

            }catch (Exception e){e.printStackTrace();}
            finally { consumer.close();}
        }else{
            System.out.println(name + " cannot read messages. Go online!");
            return null;
        }
        System.out.println(Arrays.asList(sendermess));
        return null;
    }



    //check the watched event data, if 1 or 2 => successful registered.
    //remove from enoll
    private void check(String path) throws KeeperException, InterruptedException {
        byte[] controlCode;
        try {
            controlCode = zoo.getData(path, null, null);
            //check if controlCode 1 or 2=> delete node under enroll
            if (Arrays.equals(ZKWriter.Control.SUCCES, controlCode)
                    || Arrays.equals(ZKWriter.Control.EXISTS, controlCode)) {
                System.out.println("inside check. Node Succesfully created/deleted , proceeding to delete from enroll/quit" + path);
                this.zoo.delete(path, -1);
            } else if (Arrays.equals(ZKWriter.Control.NEW, controlCode)){
                //case New creation... what do we do? wait
                System.out.println("the node is new, so lets wait for the manager to process it... ");
            } else if (Arrays.equals(ZKWriter.Control.FAILED, controlCode)) {
            //the creation failed, create back again?
            System.out.println("code is probably 0:" + controlCode);
            //error in control code. it might be 0. something is going wrong
        }
        else
                System.out.println("ERROR on worker watcher" + controlCode);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //we create a watcher on the status on the nodes under enrollment and quit.
    //this status are the Nodes control codes.
    @Override
    public void process(WatchedEvent event) {
        System.out.println(" >>>>>"+event.toString()+ " " +name);
        //watcher is triggered with the path+ name of node changed.
        if (event.getType() == Event.EventType.NodeDataChanged) {
            System.out.println(event.getPath() + " changed");

            if (event.getPath().contains("Enroll")) {
                //something changed in enrollment
                System.out.println("watcher triggered under enroll and this is the path:" + event.getPath());
                try {
                    this.check(event.getPath());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            } else if (event.getPath().contains("Quit")) {
                //something changed in enrollment
                System.out.println("watcher triggered under quit and this is the path:" + event.getPath());
                try {
                    this.check(event.getPath());
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        } else if (event.getType() == Event.EventType.NodeChildrenChanged) {
            if (event.getPath().contains("Online")){
                //refresh combobox and add new list
                try {
                    List usersOnline= zoo.getChildren(online, false);
                  //  userConsole.fillCombo(usersOnline);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }

        try {
            if (zoo.exists("/System/Online", false)!=null) {
                zoo.getChildren("/System/Online", this);
            }

        } catch (KeeperException.ConnectionLossException e) {
            //System.exit(0);
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (KeeperException e) {
            e.printStackTrace();
        }
    }



    static ZooKeeper zooConnect() throws IOException, InterruptedException {


        // Ask user in console for IP and port of Zookeeper WRITER
        Scanner sc = new Scanner(System.in);
        System.out.println("******** WELCOME TO ZOOKEEPER ********");
        System.out.print("Please provide the IP:Port of Zookeeper for WRITER (Example 127.0.0.1:2181) = ");
        String ipPortName = sc.next();
        System.out.println("Your IP:Port for Zookeeper, WRITER will be: " + ipPortName);

        /*// Ask user in console for IP and port of KAFKA CONSUMER/PRODUCER
        Scanner sc = new Scanner(System.in);
        System.out.println("******** WELCOME TO KAFKA CONSUMER/PRODUCER ********");
        System.out.print("Please provide the IP:Port of KAFKA for CONSUMER/PRODUCER (Example 127.0.0.1:9092) = ");
        String kafkaIpPortName = sc.next();
        System.out.println("Your IP:Port for KAFKA, CONSUMER/PRODUCER will be: " + kafkaIpPortName);*/

        String host = ipPortName;
        int sessionTimeout = 3000;
        final CountDownLatch connectionLatch = new CountDownLatch(1);

        //create a connection
        ZooKeeper zoo = new ZooKeeper(host, sessionTimeout, new Watcher() {

            @Override
            public void process(WatchedEvent we) {

                if (we.getState() == Event.KeeperState.SyncConnected) {
                    connectionLatch.countDown();
                }

            }
        });

        connectionLatch.await(10, TimeUnit.SECONDS);
        return zoo;
    }
}
