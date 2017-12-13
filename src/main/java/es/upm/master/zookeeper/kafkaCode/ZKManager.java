package es.upm.master.zookeeper.kafkaCode;

import org.apache.zookeeper.*;
import org.apache.zookeeper.Watcher.Event.EventType;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;


public class ZKManager implements Watcher{

    private  Stat stat;
    private  ZooKeeper zoo;
    public List<String> regList;
    private String enroll = "/System/Request/Enroll/";
    private String online = "/System/Online/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";

    interface Control {
        byte[] NEW = "-1".getBytes();
        byte[] FAILED = "0".getBytes();
        byte[] SUCCES = "1".getBytes();
        byte[] EXISTS = "2".getBytes();
    }

    public void ZKManager() throws KeeperException, InterruptedException, IOException {

        regList= new ArrayList<String>();
        //if zoo.getchildren not null.. then this
        String auth = "user:pwd";

        this.zoo = zooConnect();    // Connects to ZooKeeper service
        this.zoo.addAuthInfo("digest",auth.getBytes());
        //delete part of the tree
        destroyTree();
        //create tree structure
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


        SetWatchers();
    }

    public void destroyTree() throws KeeperException, InterruptedException {
        destroyTreeProcess("/System/Request");
        destroyTreeProcess("/System/Online");
    }



    public void destroyTreeProcess(String treePath) throws KeeperException, InterruptedException {
        //addauth digest user:pwd ( in command line)
        String auth = "user:pwd";
        this.zoo.addAuthInfo("digest",auth.getBytes());
         // call deleteTree recursively zoo.delete

        // If node doesn't exist
        if(zoo.exists(treePath, false) == null) {
            System.out.println("WARNING: Node doesn't exist." + treePath);
            return;
        }
        else {

            List<String> children = zoo.getChildren(treePath, false);
            for( String item:children) {
                destroyTreeProcess(treePath + '/' + item);
            }
            zoo.delete(treePath, -1);
        }
    }


    @Override
    public void process(WatchedEvent event) {

        if (event.getType() == EventType.NodeCreated) {
            System.out.println(event.getPath() + " created");
            //if it comes from /enroll- run ZKManager registry

        }  else if (event.getType() == EventType.NodeDataChanged) {
            System.out.println(event.getPath()+ "changed");



        } else if (event.getType() == EventType.NodeChildrenChanged) {
            //if it comes from /enroll- run ZKManager registry
            if (event.getPath().contains("Enroll")) {
                //new children appeared- lets check the REGISTRATIONLIST
                try {
                    List<String> childrenEn = zoo.getChildren("/System/Request/Enroll", false);
                    for (String item : childrenEn) {
                        //System.out.println(item);
                        System.out.println("the list under register is :"+regList);

                        if (!regList.contains(item) || regList==null) {
                            System.out.println("node created YES processed"+ item);
                            register(item);
                        }else{
                            System.out.println("node created not processed"+ item);
                        }
                    }
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            } else if (event.getPath().contains("Quit")) {
                System.out.println("Quit watcher triggered");
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
                System.out.println("Online watcher triggered");
                String onlinepath= event.getPath();

                try {
                    onlineCheck(onlinepath);
                } catch (KeeperException e) {
                    e.printStackTrace();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

            }

            else {
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

         if (stat == null && Arrays.equals(ZKManager.Control.NEW, controlCode)) {
            System.out.println("Node under enrool has code NEW" + name);
            boolean registerStatus = registerSystem(name);
            if(registerStatus) {
                System.out.println("Registration is successful for" + name);
                zoo.setData(pathenroll, ZKManager.Control.SUCCES, -1); //change enroll code to SUCCES
                //add name to regList
                regList.add(name);
              //  for(String item1 : regList) { }

            }else{
                System.out.println("Registration is not successful for " + name);

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
            zoo.setData(pathenroll, ZKManager.Control.EXISTS, -1); //put code node already created 2
            e.printStackTrace();
        } catch (InterruptedException e) {
            registerCode=false;
            zoo.setData(pathenroll, ZKManager.Control.FAILED, -1); //put code failed 0

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
                        zoo.setData(pathDelQuit, ZKManager.Control.EXISTS, -1);
                        return;
                    } catch (Exception e1) {
                        zoo.setData(pathDelQuit, ZKManager.Control.FAILED, -1);
                        return;
                    }

                    zoo.setData(pathDelQuit, ZKManager.Control.SUCCES, -1);
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
        //Get children from node.
        List<String> chilOn = zoo.getChildren(path, false);
        for (String item : chilOn) {
            boolean bol=false;
            //see if item has code exists.
            //online put in queue the ONLINE users that are not in Queue
            if (regList.contains(item)) {
                //we create the watcher for status of
                zoo.exists(online+item, this);
                System.out.println("watcher putted under "+ item);

            }
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


    public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
        ZKManager manager = new ZKManager();
        manager.ZKManager();

        for(int i = 0; i < 10000; i++)
            Thread.sleep(20);

    }
    }