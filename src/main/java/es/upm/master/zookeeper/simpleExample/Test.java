package es.upm.master.zookeeper.simpleExample;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;


import org.apache.zookeeper.Watcher.Event.KeeperState;


public class Test {

	interface Control {
		byte[] NEW = "-1".getBytes();
		byte[] FAILED = "0".getBytes();
		byte[] SUCCES = "1".getBytes();
		byte[] EXISTS = "2".getBytes();
	}

	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {

		ZKManager manager = new ZKManager();
		manager.ZKManager();

		ZKWriter zkw = new ZKWriter();
		zkw.ZKWriter();

		//zkw.quit("Ahmed");
		//zkw.create("Mahmud");


		//*****************************************************************************************************//
		/*									HOW TO RUN THE ONLINE PART											*/
		//******************************************************************************************************//
		/*1ST RUN-  uncomment THE NEXT 2 lines, in zkManager uncomment the destroy and construct the tree*/
		zkw.create("Cris");
		zkw.quit("Ahmed");

 		/*2nd RUN- comment previous lines, also comment the destroy tree and construct tree, and uncomment the next line*/
 		//zkw.goOnline("Cris");


		Thread.sleep(20000);

	}

	static ZooKeeper zooConnect() throws IOException, InterruptedException {

		String host = "localhost:2181";
		int sessionTimeout = 3000;
		final CountDownLatch connectionLatch = new CountDownLatch(1);

		//create a connection
		ZooKeeper zoo = new ZooKeeper(host, sessionTimeout, new Watcher() {

			@Override
			public void process(WatchedEvent we) {

				if (we.getState() == KeeperState.SyncConnected) {
					connectionLatch.countDown();
				}

			}
		});

		connectionLatch.await(10, TimeUnit.SECONDS);
		return zoo;
	}

	static ZooKeeper zooConnectAuth() throws IOException,KeeperException, InterruptedException {
		String auth = "user:pwd";
		String host = "localhost:2181";
		int sessionTimeout = 3000;
		final CountDownLatch connectionLatch = new CountDownLatch(1);

		//create a connection
		ZooKeeper zoo = new ZooKeeper(host, sessionTimeout, new Watcher() {

        //zoo.addAuthInfo("digest",auth.getBytes());

			@Override
			public void process(WatchedEvent we) {

				if (we.getState() == KeeperState.SyncConnected) {
					connectionLatch.countDown();
				}

			}
		});

		connectionLatch.await(10, TimeUnit.SECONDS);
		return zoo;
	}

}
