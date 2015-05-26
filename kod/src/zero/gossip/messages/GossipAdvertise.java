package zero.gossip.messages;

import java.io.Serializable;
import java.net.InetSocketAddress;

import zero.gossip.NodeDB;

public class GossipAdvertise extends GossipMessage {
	
	/**
	 * Class that represents information about a single node
	 */
	public static class NodeInfo implements Serializable {
		public long nodehash;
		public InetSocketAddress nodecomms;
		public boolean alive;
		public long timestamp;		
		
		public String toString() {
			return String.format("[address %s:%d hash %d timestamp %d]", this.nodecomms.getHostName(), this.nodecomms.getHostName(), this.nodehash, this.timestamp);
		}
		
		public NodeDB.NodeInfo toNodeInfo() {
			NodeDB.NodeInfo k = new NodeDB.NodeInfo();
			k.alive = this.alive;
			k.timestamp = this.timestamp;
			k.nodehash = this.nodehash;
			k.nodecomms = this.nodecomms;
			return k;
		}
		
		public static GossipAdvertise.NodeInfo fromNodeInfo(NodeDB.NodeInfo n) {
			GossipAdvertise.NodeInfo ni = new GossipAdvertise.NodeInfo();
			ni.alive = n.alive;
			ni.timestamp = n.timestamp;
			ni.nodehash = n.nodehash;
			ni.nodecomms = n.nodecomms;
			return ni;
		}
	}
	
	/**
	 * If set to true, receiver will respond with it's entire node table
	 */
	public boolean spillback = false;
	
	public NodeInfo[] nodes;
	
	public static GossipAdvertise from_nodeinfo(NodeDB.NodeInfo[] nodeinfo, boolean spillback) {
		GossipAdvertise ga = new GossipAdvertise();
		ga.nodes = new GossipAdvertise.NodeInfo[nodeinfo.length];
		for (int i=0; i<ga.nodes.length; i++)
			ga.nodes[i] = GossipAdvertise.NodeInfo.fromNodeInfo(nodeinfo[i]);
		ga.spillback = spillback;
		return ga;
	}
}
