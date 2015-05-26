package zero.lfds.suzie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import zero.lfds.LFDDamagedException;
import zero.lfds.LFDResultSet;
/**
 * a result set class
 *
 * Optimization idea: use direct ByteBuffers, allocated at construction, to do seeks for locate
 */
public class SUZIEResultSet implements LFDResultSet {
	
	// Basic data about the set
	private long from;
	private long to;
	private int recsize;

	// Data access info
	private long[] blocks;
	private int seqptr = -1;	// pointer for entry in blocks
	private FileChannel cfile;	// currently readed file
	private long records_remaining;	// records remaining in this file
	
	// General management and bookkeeping
	private SUZIESeries series;
	private Path seriespath;
	
	private ByteBuffer timestamp_buffer = ByteBuffer.allocateDirect(8);
	
	public SUZIEResultSet(SUZIESeries series, long from, long to, long[] blocks, Path seriespath, int recsize) {
		this.series = series;
		this.from = from;
		this.to = to;
		this.blocks = blocks;
		this.seriespath = seriespath;
		this.recsize = recsize;
	}
	
	@Override
	public long getStartingPosition() {
		return this.from;
	}

	@Override
	public long getEndingPosition() {
		return this.to;
	}
	
	@Override
	public int getRecordSize() {
		return this.recsize;
	}
	
	@Override
	public boolean isFinished() {
		return (this.records_remaining == 0) && (this.seqptr == this.blocks.length-1);
	}
	
	/**
	 * Returns index number at which read should start/stop.
	 * 
	 * Makes no assumptions about file's position.
	 * 
	 * @param is_start Whether this is used to determine start of reading
	 * @return index number, inclusive
	 */
	private long locate(long time, boolean is_start) throws IOException {
		this.timestamp_buffer.clear();
		long imin = 0;
		long imax = this.cfile.size() / (8 + this.recsize) - 1;
		long amax = imax;
		long imid = 0;
		
		long temp = 0;
		
		while (imax >= imin) {
			imid = imin + ((imax - imin) / 2);
			this.cfile.position(imid*(8+this.recsize));
			this.cfile.read(this.timestamp_buffer);
			this.timestamp_buffer.flip();
			temp = this.timestamp_buffer.getLong();
			this.timestamp_buffer.clear();
			if (temp == time) return imid;
			if (temp < time) {
				if (imid == amax) 
					// Range exhausted right-side
					if (is_start) throw new IllegalArgumentException("Start past last record? nonsense");
					else return amax;
				imin = imid + 1;
			} else {
				if (imid == 0) 
					// Range exhausted left-side
					if (is_start) return 0;
					else throw new IllegalArgumentException("Stop before first record? nonsense");
				imax = imid - 1;
			}
		}
		
		// Still not found. Interpolate.
		if (is_start) {
			if (temp < time) return imid+1;
			else return imid;
		} else {
			if (temp < time) return imid;
			else return imid-1;
		}
	}

	/**
	 * Prepare the class internal with a fresh new loaded file
	 */
	private void load_next_file() throws IOException{
		if (this.cfile != null) this.cfile.close();
		this.seqptr++;
		this.cfile = FileChannel.open(this.seriespath.resolve(new Long(this.blocks[this.seqptr]).toString()), StandardOpenOption.READ);
		
		if (this.blocks.length == 1) {
			// Matter's simple, really.
			long index_start;
			long index_stop;
			try {
				index_start = this.locate(this.from, true);
				index_stop = this.locate(this.to, false);
			} catch (IllegalArgumentException e) {
				// that means there there's just no data..
				this.records_remaining = 0;
				return;
			}
			
			this.records_remaining = index_stop - index_start + 1;
			this.cfile.position(index_start*(8+this.recsize));
			return;
		}
		
		if (this.seqptr == 0) {
			// First load here
			long index = this.locate(this.from, true);
			this.cfile.position(index*(8+this.recsize));
			this.records_remaining = (this.cfile.size() / (8 + this.recsize)) - index;
			return;
		}
		
		if ((this.seqptr == this.blocks.length-1)) {
			// Last load here
			this.records_remaining = this.locate(this.to, false);
			this.cfile.position(0);
			return;
		}
		
		// Normal whole-file load
		this.records_remaining = (this.cfile.size() / (8 + this.recsize));
	}
	
	/**
	 * Reads a single entry from file and loads it to data
	 * @param data appended with data
	 * @return timestamp for this entry
	 */
	private long loadEntry(ByteBuffer data) throws IOException {
		this.timestamp_buffer.clear();
		this.cfile.read(this.timestamp_buffer);
		this.timestamp_buffer.flip();
		long timestamp = this.timestamp_buffer.getLong();
		data.limit(data.position()+this.recsize);
		this.cfile.read(data);
		data.limit(data.capacity());
		this.records_remaining--;
		return timestamp;
	}

	private void loadEntryRaw(ByteBuffer data) throws IOException {
		data.limit(data.position()+this.recsize+8);
		this.cfile.read(data);
		data.limit(data.capacity());
		this.records_remaining--;
	}
	
	
	public int fetch(ByteBuffer rawdata, int bufsize) throws IOException, LFDDamagedException {
		if (rawdata.capacity() < (8+this.recsize)*bufsize) throw new IllegalArgumentException("rawdata buffer too small");
		
		int readed_in = 0;
		
		while (bufsize > 0) {
			if (this.records_remaining == 0) {
				if (this.seqptr == this.blocks.length-1) {
					// End of read, finally
					return readed_in;
				} else
					this.load_next_file();
				continue;
			}
			
			this.loadEntryRaw(rawdata);
			readed_in++;
			bufsize--;
		}
		
		return readed_in;		
		
	}
	
	@Override
	public int fetch(long[] timestamps, ByteBuffer rawdata, int bufsize)
			throws IOException, LFDDamagedException {

		if (rawdata.capacity() < this.recsize*bufsize) throw new IllegalArgumentException("rawdata buffer too small");
		
		int readed_in = 0;
		
		while (bufsize > 0) {
			if (this.records_remaining == 0) {
				if (this.seqptr == this.blocks.length-1) {
					// End of read, finally
					return readed_in;
				} else
					this.load_next_file();
				continue;
			}
			
			timestamps[readed_in] = this.loadEntry(rawdata);
			readed_in++;
			bufsize--;
		}
		
		return readed_in;
	}
	
	@Override
	public void close() throws IOException {
		if (this.cfile != null)
			this.cfile.close();
		this.series.closeResultSet();
	}

}
