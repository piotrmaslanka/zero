package zero.ifc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.WritableByteChannel;

import zero.lfds.LFDDamagedException;
import zero.lfds.LFDResultSet;
import zero.store.NotFoundException;
import zero.store.SeriesController;
import zero.store.SeriesDB;
import zero.store.SeriesDefinition;
import zero.store.SeriesDefinitionDB;

/**
 * Resolves the calls using a local interface.
 * 
 * It means that those calls are meant for this node
 * 
 */
public class LocalInterface implements SystemInterface {

	/**
	 * Opens a controller, updating the series definition if necessary
	 */
	private SeriesController openController(SeriesDefinition extsd) throws IOException, DefinitionMismatchException {
		SeriesDefinition myser = null;
		myser = SeriesDefinitionDB.getInstance().getSeries(extsd.seriesName);
		
		if (extsd.doesSupersede(myser))
			throw new DefinitionMismatchException();			// outdated
		else if (extsd.equals(myser)) {
			try {
				return SeriesDB.getInstance().getSeries(extsd.seriesName);	// just fine!
			} catch (NotFoundException exc) {
				// But we just defined that before!!!!
				throw new IOException();
			}
		} else {
			SeriesDB.getInstance().redefineAsync(extsd).lock();		// superdated
			return this.openController(extsd);
		}
	}	
	
	@Override
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException {
		return SeriesDefinitionDB.getInstance().getSeries(seriesname);
	}

	@Override
	public void writeSeries(SeriesDefinition sd, long prev_timestamp,	long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException {
		SeriesController ctrl = this.openController(sd);
		if (ctrl.getSeriesDefinition().recordSize != data.length) throw new IllegalArgumentException("Data size wrong");
		
		try {
			ctrl.write(prev_timestamp, cur_timestamp, data);
		} finally {
			ctrl.close();
		} 
		
	}

	@Override
	public long getHeadTimestamp(SeriesDefinition sd) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException {
		SeriesController ctrl = this.openController(sd);		
		try {
			return ctrl.getHeadTimestamp();
		} finally {
			ctrl.close();
		}
	}

	@Override
	public void updateDefinition(SeriesDefinition sd)
			throws LinkBrokenException, IOException {
		SeriesDB.getInstance().redefineAsync(sd).lock();
	}

	@Override
	public void read(SeriesDefinition sd, long from, long to, WritableByteChannel channel)
		throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException {
		
		SeriesController ctrl = this.openController(sd);		
		try {
			LFDResultSet rs = null;
			try {
				rs = ctrl.read(from, to);
				// allocate buffers for 1024 entries
				ByteBuffer bb = ByteBuffer.allocateDirect(1024*(sd.recordSize+8));
				
				while (!rs.isFinished()) {
					bb.clear();
					try {
						rs.fetch(bb, 1024);
					} catch (LFDDamagedException e) {
						throw new IOException();
					}
					bb.flip();
					channel.write(bb);
				}
				
				bb.clear();
				bb.putLong(-1);
				bb.flip();
				channel.write(bb);
			} finally {
				rs.close();
			}
		} finally {
			ctrl.close();
		}		
		
	}

	
	@Override
	public void close() throws IOException {}


	@Override
	public void readHead(SeriesDefinition sd, WritableByteChannel channel)
			throws LinkBrokenException, IOException, SeriesNotFoundException,
			DefinitionMismatchException {

		SeriesController ctrl = this.openController(sd);		
		try {
			LFDResultSet rs = null;
			try {
				long head = ctrl.getHeadTimestamp();
				rs = ctrl.read(head, Long.MAX_VALUE);
				// allocate buffers for 1 entry
				ByteBuffer bb = ByteBuffer.allocateDirect(sd.recordSize+8);
				
				if (head != -1) {
					try {
						rs.fetch(bb, 1);
					} catch (LFDDamagedException e) {
						throw new IOException();
					}
					bb.flip();
					channel.write(bb);
				}
				
				bb.clear();
				bb.putLong(-1);
				bb.flip();
				channel.write(bb);
			} finally {
				rs.close();
			}
		} finally {
			ctrl.close();
		}						
	}
}
