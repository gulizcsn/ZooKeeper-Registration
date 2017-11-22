package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.sql.Timestamp;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ZKWriter implements Watcher{
    private Stat stat;
    public ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";
    private String online = "/System/Online/";
    private String queue = "/System/Queue/";
    private String backup ="/System/Backup/";
   // private Map<String, List<String>> sendermess = new HashMap<String, List<String>>();
    public String name;
    public UserConsole userConsole;

    interface Control {
        byte[] NEW = "-1".getBytes();
        byte[] FAILED = "0".getBytes();
        byte[] SUCCES = "1".getBytes();
        byte[] EXISTS = "2".getBytes();
    }

    public void ZKWriter(String user, UserConsole userC) throws KeeperException, InterruptedException, IOException {
        this.zoo = zooConnect();    // Connects to ZooKeeper service
        this.name=user;
        userConsole=userC;

    }
    public void zooDisconnect() throws InterruptedException {

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
                zoo.create(path,ZKWriter.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children
        if (zoo.exists(path, false) != null) {
            System.out.println("User found inside reg- creating node under quit");
            //create the node who wants to quit the system
            zoo.create(path, ZKWriter.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //Setting watcher if state  of Control changes
            zoo.exists(path , (Watcher) this);

        }else{
            System.out.println("User can not be found in the system under path" + path);
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

        Thread.sleep(60000);
        System.out.println("creating watcher under Queue");
        zoo.getChildren(queue+name, this);



    }

    public void goOffline() throws KeeperException, InterruptedException {
        String path= online + name;
        //check if user is already online
        if (zoo.exists(path, false)!=null){
            System.out.println("user "+name+"disconnecting from online");
            zoo.delete(path,-1);
            //create the node who wants to quit the system
            //zoo.setData(path, Test.Control.EXISTS, -1);

        }else {
            //create ephemeral node
            System.out.println("USER NOT ONLINE !! SO CAN'T GO OFFLINE");
        }
    }


    public void send(String receiver, String msg) throws KeeperException, InterruptedException, UnsupportedEncodingException {
        //first check if sender is online...
        stat= zoo.exists(queue+name, false);

        Stat statReceiver= zoo.exists(online+name, false);
        if (stat!=null) {
            System.out.println( "From: "+name + " -> To: " + receiver + " >>>" + msg);

            //creamos nodo ephemeral sequential under the receiver. but first check if he is connected
            if(statReceiver!=null) {
                //receiver is onloine in queue- so we put under queue+receiver
                System.out.println("Receiver" + receiver+ " is ONLINE, so we will send the message");
                //zoo.create(queue + receiver, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zoo.create(queue + receiver + "/" + name, msg.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
                //MESSAGES STRUCTURE: Queue/Receiver/Sender000000X and inside data is the message in bytes
                List<String> messages=zoo.getChildren(queue+receiver, false);
                Comparator<String> sortRule = new Comparator<String>() {
                    @Override
                    public int compare(String left, String right) {
                        return Integer.parseInt(left.substring(left.length()-10),left.length()) - Integer.parseInt((right.substring(right.length()-10,right.length()))); // use your logic
                    }
                };

                Collections.sort(messages, sortRule);

                for (String receivedm : messages) {
                    byte[] message=zoo.getData(queue + receiver + "/" + receivedm, null, null);
                    //convert byte to string
                    String strmsg = new String(message, "UTF-8");
                    //System.out.println("THis is the node:  " + receivedm +" and the message inside: "+ strmsg);
                }

            }else{
                //user is not online, we add it to backup so later he will be able to read them
                System.out.println("Receiver" + receiver+ " is OFFLINE");
                zoo.create(backup + receiver, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zoo.create(backup + receiver + "/" + msg, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            }
        }else {
            System.out.println("SENDER NOT ONLINE");
        }
    }

    public List<String> read() throws KeeperException, InterruptedException, UnsupportedEncodingException {

        List<String> sendermess = new ArrayList<String>();
        //first check if sender is online...
        stat= zoo.exists(online+name, false);
        Stat statQueue= zoo.exists(queue+name, false);
        if (stat!=null) {
            //System.out.println("This guy is online and wants to read messages: " + name );
            if(statQueue!=null) {
                List<String> messages = zoo.getChildren(queue + name, null);

                if (messages.isEmpty()) {
                    System.out.println("There is no message in queue.");
                    return null;
                }

                Comparator<String> sortRule = new Comparator<String>() {
                    @Override
                    public int compare(String left, String right) {
                        return Integer.parseInt(left.substring(left.length()-10),left.length()) - Integer.parseInt((right.substring(right.length()-10,right.length()))); // use your logic
                    }
                };

                Collections.sort(messages, sortRule);
                  for (String eachMessage : messages) {
                    String sender = eachMessage.substring(0, eachMessage.length() - 10);
                    byte[] message = zoo.getData(queue + name + "/" + eachMessage, null, null);
                    String messageMeans = new String(message, "UTF-8");


                      Date date = new Date();
                      Timestamp newTime = new Timestamp(date.getTime());

                    // delete the message from the queue as soon as it is read
                    sendermess.add("{"+ newTime +"} " + sender + ": " + messageMeans);
                    //we will delete only when consuming one sender messages
                    zoo.delete(queue + name + "/" + eachMessage, -1);
                }
                System.out.println("sending messages through watcher QUEUE");
                userConsole.addMessage(sendermess);
            }
            else{
                System.out.println("There is no queue for this user: " + name );
                //return null;
            }

        }
        else{
            System.out.println(name + " cannot read messages. Go online!");
            return null;
        }

        System.out.println(Arrays.asList(sendermess));
        return sendermess;
    }

    private static <KEY, VALUE> void put(Map<KEY, List<VALUE>> map, KEY key, VALUE value) {
        map.compute(key, (s, strings) -> strings == null ? new ArrayList<>() : strings).add(value);
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
            if (event.getPath().contains("Queue")){
                System.out.println("New Message for" + event.getPath());
                try {
                    read();
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
            }else if (event.getPath().contains("Online")){
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
            zoo.getChildren(queue+name, this);
            zoo.getChildren("/System/Online", this);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    static ZooKeeper zooConnect() throws IOException, InterruptedException {

        String host = "localhost:2181";
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
/*
    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZKWriter zkw = new ZKWriter();
        zkw.ZKWriter("Cris", this);
        zkw.create();
        Thread.sleep(1000);
        zkw.goOnline();


        ZKWriter zkw1 = new ZKWriter();
        zkw1.ZKWriter("BELUSMOR", this);
        zkw1.create();
        Thread.sleep(1000);
        zkw1.goOnline();
        Thread.sleep(1000);


        ZKWriter zkw2 = new ZKWriter();
        zkw2.ZKWriter("Ahmet", this);
        zkw2.create();
        Thread.sleep(1000);
        zkw2.goOnline();

        zkw1.send("Cris", "PERRACA");
        Thread.sleep(1000);
        zkw2.send("Cris","Helloooo I'm Ahmet");
        Thread.sleep(1000);
        zkw1.send("Cris", "bebegim");

        Thread.sleep(50000);
        //zkw.zooDisconnect();
        //zkw.goOffline();
        //Thread.sleep(50000);
        zkw.read();
       // System.out.println(message);

        Thread.sleep(50000);
    }
*/
}
