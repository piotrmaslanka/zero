package zero.ifc;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.channels.SocketChannel;

import zero.store.SeriesDefinition;
import zero.util.WorkUnit;

/**
 * work unit servicing an interface call
 *
 */
public class NetworkCallInbound extends WorkUnit {

	private SocketChannel sc;
	private SystemInterface ifc;
	
	public NetworkCallInbound(SocketChannel sc, SystemInterface ifc) {
		this.sc = sc;
		this.ifc = ifc;
	}
	
	@Override
	public void run() throws IOException {
		Socket sc = this.sc.socket();
		this.sc.configureBlocking(true);
		sc.setSoTimeout(8000);
		DataInputStream dis = new DataInputStream(sc.getInputStream());
		DataOutputStream dos = new DataOutputStream(sc.getOutputStream());
		
		try {
			while (true) {
				byte command = dis.readByte();
				
				if (command == 0) {
					String seriesName = dis.readUTF();
					SeriesDefinition sd = null;
					try {
						sd = ifc.getDefinition(seriesName);					
						if (sd == null)
							dos.writeByte((byte)2);
						else {
							dos.writeByte((byte)0);
							sd.toDataStreamasINTPRepresentation(dos);
						}
					} catch (LinkBrokenException | IOException e) {
						System.err.println("NetworkCallInbound, run, 0");
						e.printStackTrace();
						dos.writeByte((byte)1);
					}	
				}
				else if (command == 1) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						ifc.updateDefinition(sd);
						dos.writeByte((byte)0);
					} catch (LinkBrokenException | IOException e) {
						System.err.println("NetworkCallInbound, run, 1");
						e.printStackTrace();
						dos.writeByte((byte)1);
					}
				}
				else if (command == 2) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						long head = ifc.getHeadTimestamp(sd);
						dos.writeByte((byte)0);
						dos.writeLong(head);
					} catch (LinkBrokenException | IOException e) {
						System.err.println("NetworkCallInbound, run, 2, LBE");
						e.printStackTrace();
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						System.err.println("NetworkCallInbound, run, 2, SNE");
						e.printStackTrace();
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						System.err.println("NetworkCallInbound, run, 2, DME");
						e.printStackTrace();
						dos.writeByte((byte)3);
					}			
				}
				else if (command == 3) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					long prevt = dis.readLong();
					long curt = dis.readLong();
					byte[] data = new byte[dis.readInt()];
					dis.readFully(data);
					
					try {
						ifc.writeSeries(sd, prevt, curt, data);
						dos.writeByte((byte)0);
					} catch (LinkBrokenException | IOException e) {
						System.err.println("NetworkCallInbound, run, 3, IOE");
						e.printStackTrace();
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						System.err.println("NetworkCallInbound, run, 3, SNFE");
						e.printStackTrace();
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						System.err.println("NetworkCallInbound, run, 3, DME");
						e.printStackTrace();
						dos.writeByte((byte)3);
					} catch (IllegalArgumentException e) {
						System.err.println("NetworkCallInbound, run, 3, IAE");
						e.printStackTrace();
						dos.writeByte((byte)4);
					}
				} else if (command == 4) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					long from = dis.readLong();
					long to = dis.readLong();
					
					try {
						dos.writeByte((byte)0);
						ifc.read(sd, from, to, this.sc);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						dos.writeByte((byte)3);
					} catch (IllegalArgumentException e) {
						dos.writeByte((byte)4);
					}															
				} else if (command == 5) {
					SeriesDefinition sd = SeriesDefinition.fromDataStreamasINTPRepresentation(dis);
					try {
						dos.writeByte((byte)0);
						ifc.readHead(sd, this.sc);
					} catch (LinkBrokenException | IOException e) {
						dos.writeByte((byte)1);
					} catch (SeriesNotFoundException e) {
						dos.writeByte((byte)2);
					} catch (DefinitionMismatchException e) {
						dos.writeByte((byte)3);
					}
				}
				
				dos.flush();
			}
		} catch (Exception e) {
		} finally {
			sc.close();
			ifc.close();
		}
	}

}
