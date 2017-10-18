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

import es.upm.master.zookeeper.SimpleWatcher;

import org.apache.zookeeper.Watcher.Event.KeeperState;

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

		CreateTree tree = new CreateTree();
		tree.constructTree(zoo);

		//create znode
		//zoo.create("/test", "znode".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
		//create znode sequential
		//zoo.create("/test/sequential", "znode_sequential".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
		//create znode ephemereal
		//zoo.create("/test/ephemeral", "znode_ephemeral".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

	}


}
