package zero.store;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

import zero.lfds.LFDAlreadyExistsException;
import zero.lfds.LFDDamagedException;
import zero.lfds.LFDException;
import zero.lfds.LFDNotFoundException;
import zero.lfds.LFDOpenedException;
import zero.lfds.LFDResultSet;
import zero.lfds.LFDSeries;
import zero.lfds.suzie.SUZIEDriver;
import zero.startup.ConfigManager;

/**
 * Class that oversees a write-aheading for given series
*
 */
public class WriteAheadContext {
	final private SeriesDefinition sernfo;
	final private LFDSeries primary_storage;

	/**
	 * Driver holding partials. Partials are backed by reasonably fast and simple SUZIE
	 * Not null only if series is in WRITEAHEAD mode
	 */
	private SUZIEDriver partial_storage;
	/**
	 * Each partial's name is the timestamp which precedes first write in the partial
	 */
	final private List<Long> partials_l = new LinkedList<>();			// repair partials, sorted ascending
	final private List<LFDSeries> partials = new LinkedList<>();		// repair partials, sorted ascending
	
	/**
	 * Initializes a WAC for particular series
	 * @param series series definition
	 * @param primary_storage A series for this WAC. 
	 * 		  It's WAC's responsibility to close it later
	 */
	public WriteAheadContext(SeriesDefinition series, LFDSeries primary_storage) throws IOException {		
		this.sernfo = series;
		this.primary_storage = primary_storage;
		
		if (Files.exists(ConfigManager.get().repair_datapath.resolve(this.sernfo.seriesName))) {
			// we have a broken series
			this.partial_storage = new SUZIEDriver(ConfigManager.get().repair_datapath.resolve(this.sernfo.seriesName));
			
			// load all partials - just the information about existence
			try {
				for (String partial : this.partial_storage.enumerateSeries())
					this.partials_l.add(Long.decode(partial));
			} catch (NumberFormatException e) {
				throw new IOException();
			}

			Collections.sort(this.partials_l);			
			
			// load all LFD partial series
			try {
				for (Long partial : this.partials_l)
					this.partials.add(this.partial_storage.getSeries(partial.toString()));
			} catch (LFDException e) {
				throw new IOException();
			}
		}		
	}
	
	/**
	 * A new storage for partials needs to be created
	 */
	private void createPartialStorage() throws IOException {
		Path dir = Files.createDirectory(ConfigManager.get().repair_datapath.resolve(this.sernfo.seriesName));
		this.partial_storage = new SUZIEDriver(dir);
		System.err.println("Partial storage created for "+this.sernfo.seriesName);
	}
	
	/**
	 * Deletes the storage for partials
	 */
	private void deletePartialStorage() throws IOException {
		try {
			this.partial_storage.close();
		} catch (LFDOpenedException e) {
			throw new RuntimeException("Should not happen");
			
		}
		Files.delete(ConfigManager.get().repair_datapath.resolve(this.sernfo.seriesName));
		
		this.partial_storage = null;
		System.err.println("Partial storage killed for "+this.sernfo.seriesName);
	}
	
	/**
	 * Allocates a new partial and adds it to lists
	 * @param timestamp timestamp of the write preceding writes in this partial
	 */
	private void createNewPartial(long timestamp) throws IOException {
		this.partials_l.add(timestamp);
		try {
			this.partials.add(
				this.partial_storage.createSeries(new Long(timestamp).toString(), this.primary_storage.getRecordSize(), "")
			);
		} catch (LFDAlreadyExistsException e) {
			throw new RuntimeException("Should not happen");
		}
	}
	
	/**
	 * Returns whether partial of given name exists
	 * @param from partial name
	 * @return whether partial exists
	 */
	public boolean partialExists(long from) {
		for (long pname : this.partials_l)
			if (from == pname)
				return true;
		return false;
	}
	
	
	/**
	 * A write that is for sure expected to hit WA
	 * @throws IllegalArgumentException this is a write that previously hit WA
	 * 		   or just plain doesn't make sense
	 */
	public void write(long prev_timestamp, long cur_timestamp, byte[] data) throws IllegalArgumentException, IOException {
		// A partial is supposed to have at least one record inside, so it has a meaningful HEAD
		// So far, set up the environment that in every case we need just to pop a write to 
		// last partial on the list

		LFDSeries partial = null;

		if (!this.needsRepair()) {
			this.createPartialStorage();
			this.createNewPartial(prev_timestamp);
			partial = this.partials.get(this.partials.size()-1);
		} else
			if (this.partials.get(this.partials.size()-1).getHeadTimestamp() < prev_timestamp) { 
				createNewPartial(prev_timestamp);
				partial = this.partials.get(this.partials.size()-1);
			} else	// Now it's totally out of order and nuts. Let's try to make sense out of it..
				for (int i=this.partials.size()-1; i>=0; i--)
					if (this.partials.get(i).getHeadTimestamp() == prev_timestamp) {
						partial = this.partials.get(i);
						break;
					}
		
		// if partial is null that means this is a duplicating write. Ignore it.
		if (partial == null) return;
		
		// Write
		partial.write(cur_timestamp, data);
	}
	
	/**
	 * Close this WAC
	 */
	public void close() throws IOException {
		try {
			this.primary_storage.close();	// Shut down primary storage series
			for (LFDSeries partial : this.partials) partial.close(); // Shut down all partial series
			if (this.partial_storage != null)
				this.partial_storage.close();	// Shut down partial storage		
		} catch (LFDOpenedException e) {
			throw new RuntimeException("Should not happen");
		}
	}
	
	private void transcribePartial(int index) throws IOException {
		LFDSeries partial = this.partials.get(index);
		
		this.partials.remove(index);
		this.partials_l.remove(index);
		
		// Transcribe it now..
		try {
			LFDResultSet data = partial.readAll();
			
			long[] timestamps = new long[1];
			ByteBuffer buf = ByteBuffer.allocate(partial.getRecordSize());
			byte[] ba = buf.array();		// get the backing array
			
			while (data.fetch(timestamps, buf, 1) == 1) {
				this.primary_storage.write(timestamps[0], ba);
				buf.clear();
			}
			
			data.close();
		} catch (LFDDamagedException e) {
			throw new IOException();
		}			
		
		// Data transcribed, delete this partial now...
		String partname = partial.getName();
		try {
			partial.close();
			this.partial_storage.deleteSeries(partname);
		} catch (LFDOpenedException | LFDNotFoundException e) {
			throw new RuntimeException("Hey, I've just closed it now! :(");
		}			
		
		// Have all partials been processed?
		if (this.partials_l.size() == 0)
			this.deletePartialStorage();		// Great, delete the partial storage now!
			
	}
	
	/**
	 * Signal from upper layer that a particular timestamp was written to 
	 * primary storage
	 * @param timestamp timestamp of the write
	 */
	public void signalWrite(long timestamp) throws IOException {
		// this is written as a while loop, because out-of-order
		// "write-aheading" can result in a single write cascading a few
		// partials together
		while (this.partials.size() > 0) {
			if (this.partials_l.get(0) == timestamp) {
				// get next timestamp, because next time this if is called
				// it will make sense
				timestamp = this.partials.get(0).getHeadTimestamp();
				this.transcribePartial(0);				
			}
			else
				break;
		}
	}
	
	/**
	 * Returns whether this series needs to be repaired
	 * @return whether this series needs to be repaired
	 */
	public boolean needsRepair() {
		return this.partial_storage != null;
	}
}
