package zero.gossip.messages;

import java.net.InetSocketAddress;

import zero.startup.ConfigManager;

/**
 * Routinely used to check whether other hosts are alive
 */
public class GossipHeartbeat extends GossipMessage {
	/**
	 * Access info to the sender of the heartbeat
	 */
	public InetSocketAddress sendersData;
	
	public GossipHeartbeat() {
		this.sendersData = ConfigManager.get().node_interface;
	}
}
