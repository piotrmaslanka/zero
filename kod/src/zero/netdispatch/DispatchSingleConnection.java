package zero.netdispatch;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.SocketChannel;

import zero.gossip.GossipInbound;
import zero.ifc.ClientInterface;
import zero.ifc.LocalInterface;
import zero.ifc.NetworkCallInbound;
import zero.util.WorkUnit;

/**
 * Analyzes a connection and transitions to other WorkUnit to 
 * perform some work on it later
 * 
 * @author Henrietta
 *
 */
public class DispatchSingleConnection extends WorkUnit {

	private SocketChannel sc;

	public DispatchSingleConnection(SocketChannel sc) {
		this.sc = sc;
	}

	public void runLogic() throws IOException {		
		ByteBuffer c = ByteBuffer.allocate(1);
		this.sc.read(c);
		c.flip();
		switch (c.get()) {
			case 0:
				break;
			case 1:
				new GossipInbound(this.sc).run();
				break;
			case 2:
				new NetworkCallInbound(this.sc, new LocalInterface()).run();
				break;
			case 3:
				new NetworkCallInbound(this.sc, new ClientInterface()).run();
				break;
			default:
				break;
		}
	}
	
	@Override
	public void run() {
		try {
			this.runLogic();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
