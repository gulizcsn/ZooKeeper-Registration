package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ZKWriter implements Watcher{
    private static Stat stat;
    private static ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";


    public void ZKWriter() throws KeeperException, InterruptedException, IOException {
        this.zoo = Test.zooConnect();    // Connects to ZooKeeper service

    }



    public void create(String name) throws KeeperException, InterruptedException {
        String path = enroll + name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children

        //first we check if node exists
        stat = this.getZNodeStatsReg(name);

        if (stat != null) {
            //if exists
            System.out.println("User already registered");
        } else {

            System.out.println("User not registered, proceeding to enroll");
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

    //we check registry to see if the user is already registered
    public Stat getZNodeStatsReg(String name) throws KeeperException,
            InterruptedException {
        String path = registry + name;
        stat = zoo.exists(path, true);
        return stat;
    }


    public void quit(String name) throws KeeperException, InterruptedException {
        String path = quit + name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children
        stat = this.getZNodeStatsReg(name);
        if (stat != null) {
            System.out.println("User found inside reg- creating node under quit");
            //create the node who wants to quit the system
            zoo.create(path, Test.Control.NEW , ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            //Setting watcher if state  of Control changes
            zoo.exists(path , (Watcher) this);

        }else{
            System.out.println("User can not be found in the system under path" + path);
        }
    }


    //check the watched event data, if 1 or 2 => successful registered.
    //remove from enoll
    private void check(String path) throws KeeperException, InterruptedException {
        byte[] controlCode;
        try {
            controlCode = zoo.getData(path, null, null);
            //check if controlCode 1 or 2=> Success or
            if (Arrays.equals(Test.Control.SUCCES, controlCode)
                    || Arrays.equals(Test.Control.EXISTS, controlCode)) {
                System.out.println("inside check. Node Succesfully created/deleted , proceeding to delete from enroll/quit");
                this.zoo.delete(path, -1);
            } else if (Arrays.equals(Test.Control.NEW, controlCode)){
                //case New creation... what do we do? wait
                System.out.println("the node is new, so lets wait for the manager to process it... ");
            } else if (Arrays.equals(Test.Control.FAILED, controlCode)) {
            //the creation failed, create back again?
            System.out.println("code is probably 0:" + controlCode);
            //error in control code. it might be 0. something is going wrong
        }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    //we create a watcher on the status on the nodes under enrollment and quit.
    //this status are the Nodes control codes.
    @Override
    public void process(WatchedEvent event) {
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
            //else
            //System.out.println("Error on "+ watchedEvent.getPath() + " with event " + watchedEvent.getType());
        }
    }
}
