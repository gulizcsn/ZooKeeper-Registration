package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZKWriter implements Watcher{
    private Stat stat;
    private ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";
    private String online = "/System/Online/";
    private String queue = "/System/Queue/";
    private String backup ="/System/Backup/";

    public String name;


    public void ZKWriter(String user) throws KeeperException, InterruptedException, IOException {
        this.zoo = Test.zooConnect();    // Connects to ZooKeeper service
        this.name=user;

    }



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
                zoo.create(path,Test.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                //Setting watcher if state  of Control changes
                zoo.exists(path , (Watcher) this);
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
            zoo.create(path, Test.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
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
            zoo.create(path, Test.Control.NEW, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public void goOffline() throws KeeperException, InterruptedException {
        String path= online + name;
        //check if user is already online
        if (zoo.exists(path, false)!=null){

            System.out.println("user "+name+"disconnecting from online");
            //create the node who wants to quit the system
            zoo.setData(path, Test.Control.EXISTS, -1);
            //zoo.delete(path, -1);
            //Setting watcher if state  of Control changes

            // Thread.sleep(100);

        }else {
            //create ephemeral node
            System.out.println("USER NOT ONLINE !! SO CAN'T GO OFFLINE");
        }
    }


    public void send(String receiver, String msg) throws KeeperException, InterruptedException {
        //first check if sender is online...
        stat= zoo.exists(queue+name, false);
        Stat statReceiver= zoo.exists(queue+name, false);
        if (stat!=null) {
            System.out.println( "From: "+name + " -> To: " + receiver + " >>>" + msg);

            //creamos nodo ephemeral sequential under the receiver. but first check if he is connected
            if(statReceiver!=null) {
                //receiver is onloine in queue- so we put under queue+receiver
                System.out.println("Receiver" + receiver+ " is ONLINE");
                //zoo.create(queue + receiver, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zoo.create(queue + receiver + "/" + msg, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
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

    public void read() throws KeeperException, InterruptedException {
        /*A user will request to read his messages
        1ST- check if the user is online in Queue
        2ND- check if there are messages fro him
        3rd. get the messages, remove the
         */

    }


    //check the watched event data, if 1 or 2 => successful registered.
    //remove from enoll
    private void check(String path) throws KeeperException, InterruptedException {
        byte[] controlCode;
        try {
            controlCode = zoo.getData(path, null, null);
            //check if controlCode 1 or 2=> delete node under enroll
            if (Arrays.equals(Test.Control.SUCCES, controlCode)
                    || Arrays.equals(Test.Control.EXISTS, controlCode)) {
                System.out.println("inside check. Node Succesfully created/deleted , proceeding to delete from enroll/quit" + path);
                this.zoo.delete(path, -1);
            } else if (Arrays.equals(Test.Control.NEW, controlCode)){
                //case New creation... what do we do? wait
                System.out.println("the node is new, so lets wait for the manager to process it... ");
            } else if (Arrays.equals(Test.Control.FAILED, controlCode)) {
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
        }
    }
}
