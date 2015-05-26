package zero.gossip;

import java.util.Vector;
import java.util.concurrent.ConcurrentLinkedDeque;

import zero.gossip.messages.GossipAdvertise;
import zero.gossip.messages.GossipHeartbeat;
import zero.gossip.messages.GossipMessage;

public class GossipThread extends Thread {

	private static GossipThread instance = null;
	private boolean terminating = false;
	
	public static GossipThread getInstance() {
		if (GossipThread.instance == null) GossipThread.instance = new GossipThread();
		return GossipThread.instance;
	}
	
	protected ConcurrentLinkedDeque<NodeDB.NodeInfo> fresh_data = new ConcurrentLinkedDeque<>();	// data not yet replicated to others
	
	public void terminate() { this.terminating = true; }
	
	/**
	 * Added when somebody considers that new info about node n should be replicated
	 * @param n new nodeinfo
	 */
	public void replicateNodeInfo(NodeDB.NodeInfo n) {
		this.fresh_data.add(n);
	}
	
	@Override
	public void run() {
		System.out.println("GOSSIP: Gossip thread starting");
		while (!this.terminating) {
			try {
				Thread.sleep(4000);
			} catch (InterruptedException e) { return; }
			
			Vector<NodeDB.NodeInfo> to_propagate = new Vector<>();
			while (this.fresh_data.size() > 0) to_propagate.add(this.fresh_data.remove());
			
			GossipMessage gam = null;
			if (to_propagate.size() > 0) {
				NodeDB.NodeInfo[] to_propagate_v = to_propagate.toArray(new NodeDB.NodeInfo[to_propagate.size()]);
				gam = GossipAdvertise.from_nodeinfo(to_propagate_v, false);
			}
			else
				gam = new GossipHeartbeat();
			
			NodeDB.NodeInfo[] nodes_to_talk_to = NodeDB.getInstance().getNodesToGossipTo(to_propagate.size() > 0);
			for (NodeDB.NodeInfo target : nodes_to_talk_to)
				new GossipOutbound(gam, target).executeAsThread();
			
			to_propagate = null;

		}
		System.out.println("GOSSIP: Gossip thread stopping");
	}
	
	
}
