package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;

public class ZKWriter {
    private static Stat stat;
    private static ZooKeeper zoo;
    private String enroll = "/System/Request/Enroll/";
    private String registry = "/System/Registry/";
    private String quit = "/System/Request/Quit/";

    public void ZKManager() throws KeeperException, InterruptedException, IOException {
        this.zoo = Test.zooConnect();    // Connects to ZooKeeper service



    }


    public void create(String name) throws KeeperException, InterruptedException {
        String path = enroll + name;
        //we check if node exists under the registry node "/System/Registry" with the status Stat, not listing children
        stat = this.getZNodeStatsReg(name);

        System.out.println(stat);

        if (stat != null) {
            System.out.println("User already registered");
        } else {
            System.out.println("User not registered, proceeding to register");
            System.out.println("this is the path" + path);
            //creates the first node called Bitch
            zoo.create(path, "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        }
    }


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
        System.out.println(stat);
        if (stat != null) {
            System.out.println("User is in the system");
            System.out.println(stat);
            //delete the node who wants to quit the system
            zoo.delete(path,zoo.exists(path,true).getVersion());

        } else {
            System.out.println("User can not be found in the system");
            System.out.println("this is the path" + path);

        }
    }

}
