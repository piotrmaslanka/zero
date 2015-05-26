package zero.gossip;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.SocketChannel;

import zero.gossip.messages.GossipMessage;
import zero.util.WorkUnit;

public class GossipOutbound extends WorkUnit {

	private GossipMessage m;
	private NodeDB.NodeInfo node;
	private InetSocketAddress target;
	private boolean retry = false;
	
	private static final int GOSSIP_TIMEOUT = 4000;
	
	public GossipOutbound(GossipMessage m, NodeDB.NodeInfo node) {
		this.m = m;
		this.node = node;
		this.target = this.node.nodecomms;
	}
	public GossipOutbound(GossipMessage m, InetSocketAddress target) {
		this.m = m;
		this.target = target;
		this.retry = true;	// because this is bootstrap!
	}
	
	public void runLogic() throws IOException {
		SocketChannel sc = SocketChannel.open();
		sc.socket().setSoTimeout(GOSSIP_TIMEOUT);
		sc.socket().connect(this.target, GOSSIP_TIMEOUT);
		
		ByteArrayOutputStream bos = new ByteArrayOutputStream();
		ObjectOutputStream oos = new ObjectOutputStream(bos);
		oos.writeObject(this.m);
		
		byte[] obj = bos.toByteArray();

		ByteBuffer szm = ByteBuffer.allocate(5);
		
		szm.put((byte)1);
		szm.putInt(obj.length);
		szm.flip();
		
		sc.write(new ByteBuffer[] { szm, ByteBuffer.wrap(obj) });
		sc.close();
	}

	@Override
	public void run() {
		try {
			this.runLogic();
		} catch (IOException e) {
			if (this.retry) 
				this.run();
			
			if (this.node != null)
				FailureDetector.getInstance().onFailure(this.node.nodehash);
			
			return;
		}
		
		if (this.node != null)
			FailureDetector.getInstance().onSuccess(this.node.nodehash);
	}

}
