package zero.netdispatch;

import java.io.File;
import java.io.IOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;

import zero.startup.ConfigManager;

/**
 * Thread that takes in and dispatches all inbound connections
 * that appear on node's IP
 * @author Henrietta
 *
 */
public class DispatcherThread extends Thread {
	
	public void terminate() {
		this.interrupt();
	}
	
	public DispatcherThread() {
	}
	
	public void runLogic() throws IOException {
		ServerSocketChannel sc;
		sc = ServerSocketChannel.open();
		sc.bind(ConfigManager.get().node_interface);

		while (!Thread.interrupted())
			new DispatchSingleConnection(sc.accept()).executeAsThread();
	}
	
	@Override
	public void run() {
		System.out.println("NETDISPATCH: Network dispatch thread starting");		
		try {
			this.runLogic();
		} catch (ClosedByInterruptException e) {}
		  catch (IOException e) {
			  e.printStackTrace();
		  }
	}

}
