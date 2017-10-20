package es.upm.master.zookeeper.simpleExample;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.ZKUtil;


import es.upm.master.zookeeper.SimpleWatcher;

import org.apache.zookeeper.Watcher.Event.KeeperState;
import org.apache.zookeeper.data.Stat;


public class Test {
	
	
	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {
		
		
		String host = "localhost:2181";
		int sessionTimeout = 3000;
		final CountDownLatch connectionLatch = new CountDownLatch(1);
		 
		//create a connection
		ZooKeeper zoo = new ZooKeeper(host, sessionTimeout , new Watcher() {

			@Override
			public void process(WatchedEvent we) {

				if(we.getState() == KeeperState.SyncConnected){
					connectionLatch.countDown();
				}

			}
		});

		connectionLatch.await(10, TimeUnit.SECONDS);
        Stat stat = zoo.exists("/System", true);
        System.out.println("this is the value of stat in the Initial path /System" + stat);
		if(stat!= null) {
            ZKUtil util = new ZKUtil();
            util.deleteRecursive(zoo, "/System");
        }

		CreateTree tree = new CreateTree();
		tree.constructTree(zoo);

		ZKManager manager = new ZKManager();
        //menu for deciding what to do inside class manager- we should move it
		manager.ZKManager(zoo);



	}


}
