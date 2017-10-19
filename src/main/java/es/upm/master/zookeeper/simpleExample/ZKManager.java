package es.upm.master.zookeeper.simpleExample;

import java.io.IOException;
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


import es.upm.master.zookeeper.SimpleWatcher;

        import org.apache.zookeeper.Watcher.Event.KeeperState;

/*first of all, lets try to add a new ID under request/enroll
*/


public class ZKManager {
    private static Stat stat;

    public void ZKManager(ZooKeeper zoo) throws KeeperException, InterruptedException {
        //calling the methon create and giving the original connection zoo, and the user name
        create("Belus",zoo);
    }

     //I created variables for paths.
        private String enroll = "/System/Request/Enroll/";
        private String registry = "/System/Registry/";

        //create menu for deciding what to do.
        /**
         * 1-create user (enroll)
         * 2 quit user
         *
         *
         *
         * if user chose ==1
         * create("nombre_user")  (this node should be ephemeral)
         */


    public void create(String name, ZooKeeper zoo) throws KeeperException, InterruptedException {
        String path = enroll + name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children
        stat = this.getZNodeStatsReg(name, zoo);
        if (stat != null) {
            System.out.println("User already registered");
        } else {
            System.out.println("User not registered, proceeding to register");
            System.out.println("this is the path" + path);
            //creates the first node called Bitch
            zoo.create(path, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }

    public Stat getZNodeStatsReg(String name, ZooKeeper zoo) throws KeeperException,
            InterruptedException {
        String path = registry + name;
        stat = zoo.exists(path, true);
        return stat;


    }

/*

            public List<String> getZNodeChildren(String path) throws KeeperException,
                    InterruptedException{

                List<String> children  = null;
            }
*/


}

