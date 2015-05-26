package zero.gossip;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;
import java.util.Vector;

import zero.gossip.messages.GossipAdvertise;
import zero.gossip.messages.GossipHeartbeat;
import zero.gossip.messages.GossipMessage;
import zero.util.WorkUnit;

/**
 * Processes network connection with inbound gossip
 */
public class GossipInbound extends WorkUnit {
	
	private static final int GOSSIP_ROUND_TIMEOUT = 5000;

	private SocketChannel sc;
	
	public GossipInbound(SocketChannel sc) {
		this.sc = sc;
	}
	
	public void runLogic() throws IOException, ClassNotFoundException {
		ByteBuffer b4 = ByteBuffer.allocate(4);
		GossipMessage m = null;
		InetAddress source = null;

		this.sc.socket().setSoTimeout(GOSSIP_ROUND_TIMEOUT);
		
		while (b4.position() < 4) this.sc.read(b4);		// read size
		
		b4.flip();
		int transferred_object_size = b4.getInt();
		
		ByteBuffer objbuf = ByteBuffer.allocate(transferred_object_size);
		while (objbuf.position() < transferred_object_size) this.sc.read(objbuf);
		objbuf.flip();

		ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(objbuf.array()));
		
		m = (GossipMessage)ois.readObject();
		
		source = this.sc.socket().getInetAddress();
		this.sc.close();	// no further processing to be done on this socket
					
		if (m instanceof GossipAdvertise) {
			GossipAdvertise ga = (GossipAdvertise)m;
			NodeDB.NodeInfo[] nodes = new NodeDB.NodeInfo[ga.nodes.length];
			for (int i=0; i<ga.nodes.length; i++)
				nodes[i] = ga.nodes[i].toNodeInfo();

			NodeDB.getInstance().update(nodes);
				
			if (ga.spillback) {			
				// formulate an advertisement message
				GossipAdvertise gam = GossipAdvertise.from_nodeinfo(NodeDB.getInstance().dump(), false);
				new GossipOutbound(gam, NodeDB.getInstance().getNodeByInetAddress(source)).run();
			}
		}
		
		if (m instanceof GossipHeartbeat)
			// Do I know the sender?
			if (NodeDB.getInstance().getNodeByInetAddress(source) == null) {
				// I don't know this node! I will sent a GossipAdvertise with my database and a spillback request...
				GossipAdvertise gam = GossipAdvertise.from_nodeinfo(NodeDB.getInstance().dump(), true);
				new GossipOutbound(gam, ((GossipHeartbeat)m).sendersData).run();				
			}

	}
	
	@Override
	public void run() {
		try {
			this.runLogic();
		} catch (IOException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
