package zero.startup;

import java.io.IOException;

import zero.gcollector.GarbageCollectionThread;
import zero.gossip.GossipOutbound;
import zero.gossip.GossipThread;
import zero.gossip.NodeDB;
import zero.gossip.messages.GossipAdvertise;
import zero.netdispatch.DispatcherThread;
import zero.repair.ReparatorySupervisorThread;
import zero.store.SeriesDB;
import zero.store.SeriesDefinitionDB;

public class Run {

	
	public static void main(String[] args) throws IOException, InterruptedException {
		System.setProperty("java.net.preferIPv4Stack" , "true");
		ConfigManager.loadConfig("config.json");
		NodeDB.getInstance();				// create the NodeDB
		SeriesDefinitionDB.getInstance();	// create the SeriesDefinitionDB
		SeriesDB.getInstance();				// create the SeriesDB

		ReparatorySupervisorThread.getInstance().start();
		GossipThread.getInstance().start();
		new GarbageCollectionThread().start();
		DispatcherThread dt = new DispatcherThread();
		dt.start();
		
		System.out.println("Started Zero v1.0");
		System.out.format("Listening on %s:%d\n", ConfigManager.get().node_interface.getAddress().toString(),
						                          ConfigManager.get().node_interface.getPort());
		System.out.format("Responsiblility starts at %d\n", ConfigManager.get().nodehash);
		
		// Should we bootstrap?
		if (ConfigManager.get().bootstrap != null)
			System.out.format("Bootstrapping from %s:%d\n", ConfigManager.get().bootstrap.getAddress().toString(),
					                                        ConfigManager.get().bootstrap.getPort());
			new GossipOutbound(
					GossipAdvertise.from_nodeinfo(NodeDB.getInstance().dump(), true), 
					ConfigManager.get().bootstrap
				).executeAsThread();
		
		
		while (true) {
			Thread.sleep(10000);
		}
	}
}
