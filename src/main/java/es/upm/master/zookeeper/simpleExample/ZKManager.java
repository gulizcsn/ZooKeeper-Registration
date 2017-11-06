package es.upm.master.zookeeper.simpleExample;

import java.io.IOException;
import java.util.*;
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
    private String online = "/System/Online/";
    private String queue = "/System/Queue/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";
    private String backup ="/System/Backup/";

    public void ZKManager() throws KeeperException, InterruptedException, IOException {

        regList= new ArrayList<String>();
        //if zoo.getchildren not null.. then this
        String auth = "user:pwd";

        this.zoo = Test.zooConnect();    // Connects to ZooKeeper service
        this.zoo.addAuthInfo("digest",auth.getBytes());


        destroyTree("/System/Request");
        destroyTree("/System/Online");
        destroyTree("/System/Queue");
        destroyTree("/System"); //why destroy System?

        constructTree();

        regList= zoo.getChildren("/System/Registry",false);
        System.out.println(regList);
        this.SetWatchers();


    }

    public void constructTree() throws KeeperException, InterruptedException {

        String auth = "user:pwd";
        zoo.addAuthInfo("digest",auth.getBytes());

        try {
            //create protected node
            zoo.create("/System", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            zoo.create("/System/Registry", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
            zoo.create("/System/Backup", "znode".getBytes(), ZooDefs.Ids.CREATOR_ALL_ACL, CreateMode.PERSISTENT);
        }
        catch (KeeperException.NodeExistsException e) {      }


        //create znode sequential
        zoo.create("/System/Request", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential*/
        zoo.create("/System/Request/Enroll", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential
        zoo.create("/System/Request/Quit", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential

        zoo.create("/System/Online", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        //create znode sequential
        zoo.create("/System/Queue", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);


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
                try {
                    List<String> childrenEn = zoo.getChildren("/System/Request/Enroll", false);
                    for (String item : childrenEn) {
                        //System.out.println(item);
                        System.out.println("the list under register is :"+regList);

                            if (!regList.contains(item) || regList==null) {
                                register(item);
                            }else{}
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getPath().contains("Quit")) {
                System.out.println("W triggered inside quit");
                //to avoid complexity, we will create function quit outside the process
                String pathquit= event.getPath();
                try {
                    quit(pathquit);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }else if (event.getPath().contains("Online")) {
                System.out.println("New node inside online");
                String onlinepath= event.getPath();

                try {
                    onlineCheck(onlinepath);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getPath().contains("Queue")) {
                System.out.println("New node inside QUEUE");

            } else {
                System.out.println(event.getPath() + " what is this??");
            }

        }

        SetWatchers();
    }


     private void register(String name)throws KeeperException, InterruptedException {

        //we also need to check if item inside enroll has code NEW_CREATION.
        String path = registry + name;
        String pathenroll = enroll + name;
        Stat stat = zoo.exists(path, true);
        byte[] controlCode= zoo.getData(pathenroll, null, null); //we get

         if (stat == null && Arrays.equals(Test.Control.NEW, controlCode)) {
            System.out.println("Checking before registring, User in enroll has code NEW, so we create new node in registry");
            boolean registerStatus = registerSystem(name);
            if(registerStatus) {
                System.out.println("Registration is successful");
                zoo.setData(pathenroll, Test.Control.SUCCES, -1); //change enroll code to SUCCES
                //add name to regList
                regList.add(name);
              //  for(String item1 : regList) { }

            }else{
                System.out.println("Registration is not successful");

            }
        }
        SetWatchers();
    }


    public boolean registerSystem(String name) throws KeeperException, InterruptedException {
        String path = registry + name;
        String pathenroll= enroll+ name;
        boolean registerCode = false;
        //check register exists

        try {
            zoo.create(path, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); //Creo nodo en registry con codigo 0, ya se cambiara
            registerCode= true;
        } catch (KeeperException.NodeExistsException e) {
            registerCode=false;
            //node existi in enrolment, changing status
            zoo.setData(pathenroll, Test.Control.EXISTS, -1); //put code node already created 2
            e.printStackTrace();
        } catch (InterruptedException e) {
            registerCode=false;
            zoo.setData(pathenroll, Test.Control.FAILED, -1); //put code failed 0

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

    private void quit(String path)throws KeeperException,
            InterruptedException {
        try {
            List<String> childrenEn = zoo.getChildren("/System/Request/Quit", false);
            for (String item : childrenEn) {

                if (regList.contains(item)) {
                    String pathDelQuit= quit + item;
                    String pathDelReg= registry + item;
                    try {
                        zoo.delete(pathDelReg, -1);
                        regList.remove(item);
                        System.out.println("user succes deleted under Registry");

                    } catch (KeeperException.NoNodeException e1) {
                        //If user was not registered M: set /request/quit/w_id:2
                        zoo.setData(pathDelQuit, Test.Control.EXISTS, -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(pathDelQuit, Test.Control.FAILED, -1);
                        return;
                    }

                    zoo.setData(pathDelQuit, Test.Control.SUCCES, -1);
                    //NOW that the data inside /request/quit/item has changed, we will trigger the watcher and do..
                    // zoo.delete(pathDelQuit, -1);

                }
            }
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
    private void onlineCheck(String path)throws KeeperException, InterruptedException {

            //Shall we see if user is already in Queu? Yes we need to check if it is not already online.
            //done it in WRITER
            //Get children from node.
            List<String> chilOn = zoo.getChildren(path, false);
            for (String item : chilOn) {
                boolean bol=false;

                if (regList.contains(item)) {
                    String pathQue= queue + item;
                    //the user is in reglist or online already, so lets make him go online.

                    try {
                        zoo.create(pathQue , "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                        bol=true;


                    } catch (KeeperException.NodeExistsException e1) {
                        System.out.println("User has been online before, the node is still inside online");
                    } catch (Exception e1) { }

                    if(bol){
                        System.out.println("going to get backups");
                        this.onlineManagement(item);
                    }


                }
            }

    }

    public void onlineManagement(String name)throws KeeperException, InterruptedException {
        // Start recovery from /backup/w_id
        //this online Users management is created inside node Queue. we will watch children under backup and copy them to queue
        Stat stat= zoo.exists(backup+name, false);
        if(stat!=null){
            System.out.println("there are backuped messages ");

            List<String> backMess = zoo.getChildren(backup+name, false);
            //if list not empty. we get all the messages and move them to Queue
            for (String item : backMess) {
                String copyMess = queue + name + item;
                String delMess = backup + name + item;
                zoo.create(copyMess, "0".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
                zoo.delete(delMess, -1);
            }
        }else {
            System.out.println("No backup messages to copy");
        }
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
        try {
            zoo.getChildren("/System/Online", this);
        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        try {
            zoo.getChildren("/System/Queue", this);
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