package zero.lfds.suzie;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;

import zero.lfds.LFDDamagedException;
import zero.lfds.LFDOpenedException;
import zero.lfds.LFDResultSet;
import zero.lfds.LFDSeries;

/**
 * Honestly, close() does nothing here except telling driver about it.
 * It's drivers responsibility to clean-up
 */
public class SUZIESeries implements LFDSeries {

	final private SUZIEDriver driver;
	final private Path seriespath;
	final private String name;
	final private int recsize;
	private long head;
	/**
	 * The file that will receive the write.
	 * 
	 * Before a write it will be checked whether slabsize was exceeded
	 */
	private Path leadfile;
	private int records_in_lead;
	/**
	 * Append-opened file for outputing. Closed if equal to null.
	 */
	private FileChannel leadfile_output;
	final private Object leadfile_output_lock = new Object();
	
	/**
	 * Size of single file in records
	 * 
	 * By default calculated so that block-files are about 16 MB
	 */
	final private long slabsize;
	
	/**
	 * Pending trim. If null, then no trim is pending. Trim will be executed
	 * upon closing last result set
	 */
	private Long trim_time;
	private volatile boolean trim_in_progress = false;
	
	private AtomicInteger open_resultsets = new AtomicInteger(0);
	
	public SUZIESeries(SUZIEDriver driver, String name, int recsize, String options) throws IOException, IllegalArgumentException {
		this.name = name;
		this.driver = driver;
		this.recsize = recsize;
		this.seriespath = this.driver.base_directory.resolve(name);
		
		long maximum = Long.MIN_VALUE;
		DirectoryStream<Path> ds = Files.newDirectoryStream(this.seriespath);
		for (Path path : ds) {
			try {
				long fnam = Long.parseLong(path.getFileName().toString());
				if (fnam > maximum) maximum = fnam;
			} catch (NumberFormatException p) {}
		}
		ds.close();
		
		this.leadfile = this.seriespath.resolve(new Long(maximum).toString());
		this.records_in_lead = (int)(Files.size(this.leadfile) / (8 + this.recsize));	
		
		// Get HEAD
		FileChannel fc = FileChannel.open(this.leadfile, StandardOpenOption.READ);
		if (fc.size() > 0) {
			fc.position(fc.size()-8-this.recsize);
			ByteBuffer bc = ByteBuffer.allocate(8);
			fc.read(bc);
			bc.flip();
			this.head = bc.getLong();
		} else
			this.head = -1;
		
		fc.close();

		long _slabsize = 16777216 / (8 + recsize);
		
		// analyze options
		for (String option : options.split(";")) {
			if (options.length() == 0) continue;
			String kv[] = option.split("=");
			if (kv.length != 2) throw new IllegalArgumentException("error in options");
			if (kv[0].equals("slabsize"))
				_slabsize = Long.parseLong(kv[1]);
		}
		
		this.slabsize = _slabsize;
	}

	@Override
	public int getRecordSize() {
		return this.recsize;
	}

	@Override
	public String getName() {
		return this.name;
	}

	@Override
	public LFDResultSet read(long from, long to) throws IOException, LFDDamagedException {
		if (to < from)
			throw new IllegalArgumentException("to must be greater or equal to from");
		
		while (this.trim_in_progress) Thread.yield();

		List<Long> files = new ArrayList<Long>();
		DirectoryStream<Path> ds = Files.newDirectoryStream(this.seriespath);
		for (Path path : ds) {
			try {
				long fnam = Long.parseLong(path.getFileName().toString());
				files.add(fnam);
			} catch (NumberFormatException p) {}
		}
		ds.close();
		
		Collections.sort(files);
		
		// Locate the starting block
		int start_index = 0;
		
		while (files.get(start_index) <= from) {
			if (start_index == files.size()-1)
				// We cannot advance further. This index is the seeked-for position
				break;
			
			if (files.get(start_index+1) <= from)
				start_index++;
			else
				break;
		}
		
		// start_index is the index in files() of the block containing our starting datapoint
		// Locate the ending block
		int end_index = start_index;
		while (end_index < files.size()-1) { // while we can do any advancing...
			if (files.get(end_index+1) <= to) end_index++;
			else break;
		}
		
		// transcribe the entries
		long[] filetab = new long[end_index-start_index+1];
		for (int i=0; i<filetab.length; i++)
			filetab[i] = files.get(start_index+i);
		
		this.open_resultsets.incrementAndGet();
		
		return new SUZIEResultSet(this, from, to, filetab, this.seriespath, this.recsize);
	}

	@Override
	public long getHeadTimestamp() {
		return this.head;
	}

	@Override
	public void write(long timestamp, byte[] data) throws IllegalArgumentException, IOException { synchronized(this.leadfile_output_lock) {
		if (timestamp <= this.head) throw new IllegalArgumentException("invalid timestamp");
		if (data.length != this.recsize) throw new IllegalArgumentException("data size must be equal to recsize");
		
		boolean split_occurred = false;
		
		while (this.trim_in_progress) Thread.yield();		
		
		// Check whether leadfile requires a split
		if (this.records_in_lead >= this.slabsize) {
			// split is required
			this.closeLeadfile();
			this.leadfile = this.seriespath.resolve(new Long(timestamp).toString());

			this.leadfile_output = FileChannel.open(this.leadfile, StandardOpenOption.WRITE, StandardOpenOption.CREATE);

			this.records_in_lead = 0;
			split_occurred = true;
		}
		
		ByteBuffer out = ByteBuffer.allocate(8+this.recsize);
		out.putLong(timestamp);
		out.put(data);
		out.flip();
		
		if (this.leadfile_output == null)
			this.leadfile_output = FileChannel.open(this.leadfile, StandardOpenOption.APPEND);

	
		this.leadfile_output.write(out);
		
		this.records_in_lead++;
		this.head = timestamp;
		
		if (split_occurred) this.executeTrim();
	}}

	@Override
	public void close() throws IOException, LFDOpenedException {
		this.sync();
		if (this.open_resultsets.get() > 0)
			throw new LFDOpenedException();
		this.driver.closeSeries(this.name);
	}
	
	/**
	 * Closes the leadfile if open for output
	 */
	private void closeLeadfile() throws IOException { 
		if (this.leadfile_output == null) return; 
		this.leadfile_output.close(); 
		this.leadfile_output = null; 
	}

	@Override
	public void compactMemory() throws IOException { synchronized(this.leadfile_output_lock) {
		this.closeLeadfile();
	}}
	
	/**
	 * This may be the last method called on this instance, ever.
	 */
	@Override
	public void sync() throws IOException { synchronized(this.leadfile_output_lock) {
		this.closeLeadfile();
	}}	
	
	/**
	 * Called by result sets to signify that they are closed
	 */
	protected void closeResultSet() {
		this.open_resultsets.decrementAndGet();
		if (this.open_resultsets.get() == 0)
			try {
				this.executeTrim();
			} catch (IOException e) {
				// silence this exception
			}

	}

	/**
	 * Executes a pending trim
	 */
	private void executeTrim() throws IOException {
		if (this.trim_time == null) return;
		this.trim_in_progress = true;
		
		DirectoryStream<Path> ds = Files.newDirectoryStream(this.seriespath);
		Vector<Long> vlong = new Vector<>();
		for (Path path : ds) {
			try {
				vlong.add(Long.parseLong(path.getFileName().toString()));
			} catch (NumberFormatException p) {
				continue;
			}
		}
		ds.close();
		
		Collections.sort(vlong);
		Collections.reverse(vlong);
		
		boolean sweep_mode_engaged = false;
		for (long s : vlong) {
			if (s < this.trim_time)
				if (sweep_mode_engaged)
					Files.delete(this.seriespath.resolve(new Long(s).toString()));
				else
					sweep_mode_engaged = true;
		}
		
		
		this.trim_time = null;
		this.trim_in_progress = false;
		
	}
	
	@Override
	public void trim(long split) throws IOException {
		this.trim_time = split;
	}

	@Override
	public LFDResultSet readAll() throws IOException, LFDDamagedException {
		return this.read(0, Long.MAX_VALUE);
	}

}
