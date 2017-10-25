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


	public static void main(String[] args) throws IOException, InterruptedException, KeeperException {




		ZKManager manager = new ZKManager();
        //menu for deciding what to do inside class manager- we should move it
		manager.ZKManager();


		ZKWriter zkw = new ZKWriter();
		zkw.ZKWriter();
		zkw.create("Guliz");
        zkw.quit("Belen");






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
