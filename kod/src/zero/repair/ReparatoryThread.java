package zero.repair;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.Pipe;
import java.nio.channels.WritableByteChannel;

import zero.gossip.NodeDB;
import zero.ifc.DefinitionMismatchException;
import zero.ifc.InterfaceFactory;
import zero.ifc.LinkBrokenException;
import zero.ifc.NetworkInterface;
import zero.ifc.SeriesNotFoundException;
import zero.ifc.SystemInterface;
import zero.store.SeriesController;
import zero.store.SeriesDefinition;

/**
 * Thread that repairs given partial
 *
 */
public class ReparatoryThread extends Thread {
	

	final private SeriesController sercon;
	final private SeriesDefinition sd;
	private long from;
	private long to;
	
	public ReparatoryThread(SeriesController sercon, long from, long to) {
		super();
		this.sercon = sercon;
		this.sd = sercon.getSeriesDefinition();
		this.from = from;
		this.to = to;
	}
	
	private static class TryNodeReadingThread extends Thread {
		private SeriesDefinition sd;
		private SystemInterface ifc;
		private WritableByteChannel wc;
		private long from;
		private long to;
		public volatile boolean isFailed = false;
		public TryNodeReadingThread(SeriesDefinition sd, SystemInterface ifc, WritableByteChannel wc, long from, long to) {
			super();
			this.ifc = ifc; this.wc = wc; this.from = from; this.to = to; this.sd = sd;
		}
		public void run() {
			try {
				this.ifc.read(this.sd, this.from+1, this.to, this.wc);
			} catch (LinkBrokenException | IOException | SeriesNotFoundException | DefinitionMismatchException exc) {
				this.isFailed = true;
			} finally {
				try {
					this.wc.close();
				} catch (IOException e) {
				}
			}
		}
		
	}
	
	
	public void tryNode(NodeDB.NodeInfo node, SystemInterface ifc) throws LinkBrokenException, IOException {
		
		Pipe pipe = Pipe.open();
		pipe.sink().configureBlocking(true);
		pipe.source().configureBlocking(true);
		
		TryNodeReadingThread tnrt = new TryNodeReadingThread(this.sd, ifc, pipe.sink(), this.from, this.to);
		tnrt.start();
		
		ByteBuffer timestamp = ByteBuffer.allocate(8);
		ByteBuffer record = ByteBuffer.allocate(this.sd.recordSize);
		long previousTimestamp = this.from;
		while (true) {
			timestamp.clear();
			record.clear();
			
			try {
				if (pipe.source().read(timestamp) != 8) break;
				if (pipe.source().read(record) != this.sd.recordSize) break;
			} catch (IOException e) {
				break;
			}
			
			if (tnrt.isFailed)
				throw new IOException();
			
			timestamp.flip();
			record.flip();
			
			// means read is finished
			long thisTimestamp = timestamp.getLong();
			this.sercon.write(previousTimestamp, thisTimestamp, record.array());
			previousTimestamp = thisTimestamp;			
		}
		
		try {
			tnrt.join();
		} catch (InterruptedException e) {
		} finally {
			pipe.source().close();
		}
	}
	
	public void run() {
		try {
			
			// let's locate all the OTHER responsible nodes
			NodeDB.NodeInfo[] nodes = NodeDB.getInstance().getResponsibleNodes(this.sd.seriesName, this.sd.replicaCount);
			for (NodeDB.NodeInfo ni : nodes) {
				if (ni.isLocal) continue;
				
				SystemInterface nifc = null;
				try {
					nifc = InterfaceFactory.getInterface(ni, false);
				} catch (IOException e) {
					continue;
				}
				
				try {
					this.tryNode(ni, nifc);
				} catch (LinkBrokenException | IOException e) {
				} finally {
					try {
						nifc.close();
					} catch (IOException e) {
					}
				}
			}
			
		} finally {
			try {
				this.sercon.close();
			} catch (IOException e) {
			}
		}
	}
	
}
