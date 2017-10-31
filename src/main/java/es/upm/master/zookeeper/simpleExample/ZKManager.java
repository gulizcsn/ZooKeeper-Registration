package es.upm.master.zookeeper.simpleExample;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

        import org.apache.zookeeper.CreateMode;
        import org.apache.zookeeper.KeeperException;
        import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooKeeper;
import java.util.List;


import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.ZKUtil;


public class ZKManager implements Watcher{

    private static Stat stat;
    private static ZooKeeper zoo;
    public List<String> regList;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";

    public void ZKManager() throws KeeperException, InterruptedException, IOException {

        regList= new ArrayList<String>();
        //if zoo.getchildren not null.. then this
        String auth = "user:pwd";

        this.zoo = Test.zooConnect();    // Connects to ZooKeeper service
        this.zoo.addAuthInfo("digest",auth.getBytes());

        destroyTree("/System");
        constructTree();


        regList= zoo.getChildren("/System/Registry",false);

        SetWatchers();
    }

    public void constructTree() throws KeeperException, InterruptedException {

        String auth = "user:pwd";
        zoo.addAuthInfo("digest",auth.getBytes());
        //create protected node
        zoo.create("/System", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential
        zoo.create("/System/Request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential*/
        zoo.create("/System/Request/Enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential
        zoo.create("/System/Request/Quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential
        zoo.create("/System/Registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);

        SetWatchers();
    }

    public void destroyTree(String treePath) throws KeeperException, InterruptedException {
        //addauth digest user:pwd ( in command line)
        String auth = "user:pwd";
        this.zoo.addAuthInfo("digest",auth.getBytes());
         // call deleteTree recursively zoo.delete

        // If node doesn't exist
        if(zoo.exists(treePath, false) == null) {
            System.out.println("WARNING: Node doesn't exist. Unsuccesful attempt " + treePath);
            return;
        }
        else {

            List<String> children = zoo.getChildren(treePath, false);
            for( String item:children) {
                destroyTree(treePath + '/' + item);
            }
            zoo.delete(treePath, -1);
        }
    }



    @Override
    public void process(WatchedEvent event) {

        System.out.println("watcher triggered");

        if (event.getType() == EventType.NodeCreated) {
            System.out.println(event.getPath() + " created");
            //if it comes from /enroll- run ZKManager registry

        } else if (event.getType() == EventType.NodeDeleted) {
            System.out.println(event.getPath() + " deleted");
        } else if (event.getType() == EventType.NodeDataChanged) {
            System.out.println(event.getPath() + " changed");

        } else if (event.getType() == EventType.NodeChildrenChanged) {
            System.out.println(event.getPath() + " children created");
            //if it comes from /enroll- run ZKManager registry
            if (event.getPath().contains("Enroll")) {
                //new children appeared- lets check the REGISTRATIONLIST
                System.out.println("watcher was triggered in enroll");


                try {
                    List<String> childrenEn = zoo.getChildren("/System/Request/Enroll", false);
                    for (String item : childrenEn) {
                        //System.out.println(item);
                        //System.out.println("the list"+regList);


                            if (!regList.contains(item)) {

                                register(item);

                            }else{}
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getPath().contains("Quit")) {
                System.out.println("watcher triggered inside quit");
                try {
                    List<String> childrenEn = zoo.getChildren("/System/Request/Quit", false);
                    for (String item : childrenEn) {

                        if (regList.contains(item)) {
                            System.out.println("list checked , going to delete from enroll and reg via watcher");
                            String pathDelQuit= quit + item;
                            String pathDelReg= registry + item;

                            zoo.delete(pathDelQuit, -1);
                            zoo.delete(pathDelReg, -1);
                            regList.remove(item);

                            for(String item1 : regList)
                            {
                                System.out.println("hellooooo" +item1);
                            }
                        }
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else {
                System.out.println(event.getPath() + " what is this??");
            }

            //SetWatchers();

        }

        SetWatchers();
    }


     private void register(String name)throws KeeperException,
            InterruptedException {
        String path = registry + name;
        String delpathenroll = enroll + name;
        Stat stat = zoo.exists(path, true);
        if (stat == null) {
            System.out.println("User is not in Register- lets create it");
            boolean registerStatus = registerSystem(name);
            if(registerStatus) {
                System.out.println("Registration is successful");
                //delete name from enroll node.

                zoo.delete(delpathenroll, -1);
                //add name to regList
                regList.add(name);
                for(String item1 : regList)
                {
                    System.out.println("registereddd" +item1);
                }

            }else{
                System.out.println("Registration is not successful");
            }
        }

        SetWatchers();
    }


    public boolean registerSystem(String name) {
        String path = registry + name;
        boolean registerCode = false;
        //check register exists

        try {

            zoo.create(path, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            registerCode= true;
        } catch (KeeperException e) {
            registerCode=false;
            e.printStackTrace();
        } catch (InterruptedException e) {
            registerCode=false;
            e.printStackTrace();
        }

        return registerCode;
    }


    public Stat getZNodeStatsReg(String name) throws KeeperException,
                InterruptedException {
            String path = registry + name;
            stat = zoo.exists(path, true);
            return stat;


    }


    public void SetWatchers(){
        try {
            zoo.getChildren("/System/Request/Enroll", this);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        try {
            zoo.getChildren("/System/Request/Quit", this);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }


    }

    public List<String> getList() {
        return regList;
    }
}