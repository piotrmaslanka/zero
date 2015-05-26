package zero.ifc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import zero.gossip.NodeDB;
import zero.store.SeriesDefinition;

/**
 * Interface that serves local network requests - it. requests targeted to a single node.
 * 
 * Node asked via this interface will attempt to satisfy the request WITHOUT contacting other nodes.
 */
public class NetworkInterface implements SystemInterface {

	private NodeDB.NodeInfo nifc;
	private Socket sock;
	private DataInputStream dis;
	private DataOutputStream dos;
	
	private boolean failed = false;
	
	public boolean isAlive() {
		return (!this.sock.isClosed()) && (this.sock.isConnected()) && (!this.sock.isInputShutdown()) && (!this.sock.isOutputShutdown());
	}
	/**
	 * Connect to target node.
	 */
	public NetworkInterface(InetSocketAddress addr, NodeDB.NodeInfo nifc) throws IOException {		
		try {
			this.nifc = nifc;
			
			this.sock = new Socket();
			this.sock.connect(addr, 3000);
			this.sock.setSoTimeout(8000);
			
			this.dis = new DataInputStream(this.sock.getInputStream());
			this.dos = new DataOutputStream(this.sock.getOutputStream());
			
			this.dos.writeByte((byte)2);
			this.dos.flush();
		} catch (IOException e) {
			this.failed = true;
			throw e;
		}
	}
	
	@Override
	public void close() throws IOException {
		if (this.failed) 
			InterfaceFactory.returnConnectionFailed(nifc, this);
		else
			InterfaceFactory.returnConnection(nifc, this);
	}
	
	public void physicalClose() throws IOException {
		this.sock.close();
	}
	
	@Override
	public void writeSeries(SeriesDefinition sd, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		try {
			this.dos.writeByte((byte)3);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.writeLong(prev_timestamp);
			this.dos.writeLong(cur_timestamp);
			this.dos.writeInt(data.length);
			this.dos.write(data);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 0) return;
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			if (result == 4) throw new IllegalArgumentException();
			throw new IOException();
		} catch (IOException e) {
			System.err.println("NetworkInterface, writeseries, IOException");
			e.printStackTrace();
			this.failed = true;  
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			System.err.println("NetworkInterface, writeseries, RTException");
			e.printStackTrace();
			this.failed = true;
			throw new IOException();
		}
	}
	
	@Override
	public long getHeadTimestamp(SeriesDefinition seriesname) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException {
		try {
			this.dos.writeByte((byte)2);
			seriesname.toDataStreamasINTPRepresentation(this.dos);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			
			if (result == 0)
				return this.dis.readLong();
			else
				throw new IOException();			
		} catch (IOException e) {
			System.err.println("NetworkInterface, getHeadTimestamp, IOException");
			e.printStackTrace();			
			this.failed = true;
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			System.err.println("NetworkInterface, getHeadTimestamp, RTException");
			e.printStackTrace();			
			this.failed = true;
			throw new IOException();
		}
	}
	
	@Override
	public void updateDefinition(SeriesDefinition sd) throws LinkBrokenException, IOException {
		try {
			this.dos.writeByte((byte)1);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 0)
				return;
			else
				throw new IOException();			
		} catch (IOException e) {
			this.failed = true;
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			this.failed = true;
			throw new IOException();
		}
	}
	
	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		try {
			this.dos.writeByte((byte)0);
			this.dos.writeUTF(seriesname);
			this.dos.flush();
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 0)
				return SeriesDefinition.fromDataStreamasINTPRepresentation(this.dis);
			else
				throw new IOException();
		} catch (IOException e) {
			System.err.println("NetworkInterface, getDefinition, IOException");
			e.printStackTrace();			
			
			this.failed = true;
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			System.err.println("NetworkInterface, getDefinition, RTException");
			e.printStackTrace();			

			this.failed = true;
			throw new IOException();
		}
	}
	
	@Override
	public void read(SeriesDefinition sd, long from, long to, WritableByteChannel channel)
		throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException {
		
		try {
			this.dos.writeByte((byte)4);
			sd.toDataStreamasINTPRepresentation(this.dos);
			this.dos.writeLong(from);
			this.dos.writeLong(to);
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			if (result == 4) throw new IllegalArgumentException();
			
			// rolling in
			long ts = this.dis.readLong();
			ByteBuffer record = ByteBuffer.allocate(sd.recordSize+8);
			byte[] rec = new byte[sd.recordSize];
			while (ts != -1) {
				record.clear();
				// roll one record
				record.putLong(ts);
				this.dis.readFully(rec);
				record.put(rec);
				record.flip();	
				channel.write(record);
				
				// read next timestamp
				ts = this.dis.readLong();
			}
			
			record.clear();
			record.putLong(-1);
			record.flip();
			channel.write(record);

		} catch (IOException e) {
			this.failed = true;
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			this.failed = true;
			throw new IOException();
		}
	}

	@Override
	public void readHead(SeriesDefinition sd, WritableByteChannel channel)
			throws LinkBrokenException, IOException, SeriesNotFoundException,
			DefinitionMismatchException {
		
		try {
			this.dos.writeByte((byte)5);
			sd.toDataStreamasINTPRepresentation(this.dos);
			
			int result = this.dis.readByte();
			if (result == 1) throw new RuntimeException();
			if (result == 2) throw new SeriesNotFoundException();
			if (result == 3) throw new DefinitionMismatchException();
			if (result == 4) throw new IllegalArgumentException();
			
			// rolling in
			long ts = this.dis.readLong();
			ByteBuffer record = ByteBuffer.allocate(sd.recordSize+8);
			byte[] rec = new byte[sd.recordSize];
			while (ts != -1) {
				record.clear();
				// roll one record
				record.putLong(ts);
				this.dis.readFully(rec);
				record.put(rec);
				record.flip();	
				channel.write(record);
				
				// read next timestamp
				ts = this.dis.readLong();
			}
			
			record.clear();
			record.putLong(-1);
			record.flip();
			channel.write(record);

		} catch (IOException e) {
			this.failed = true;
			throw new LinkBrokenException();
		} catch (RuntimeException e) {
			this.failed = true;
			throw new IOException();
		}
		
	}
}
