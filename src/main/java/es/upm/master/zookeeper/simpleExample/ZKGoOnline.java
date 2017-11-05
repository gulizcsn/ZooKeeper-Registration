package es.upm.master.zookeeper.simpleExample;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;

public class ZKGoOnline {

    private static Stat stat;
    private static ZooKeeper zoo;
    private String online = "/System/Online/";

    //we create a watcher on the status on the nodes under childrens of /onlinw¿e
    //this status are the Nodes control codes.
    /*@Override
    public void process(WatchedEvent event) {
        //watcher is triggered with the path+ name of node changed.
        if (event.getType() == Watcher.Event.EventType.NodeDataChanged) {
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
*/

}
