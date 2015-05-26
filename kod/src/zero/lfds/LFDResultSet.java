package zero.lfds;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Class that represents data readed from given series by
 * means of a read query.
 * 
 * If it a class so that not all data is loaded into memory at once
 */
public interface LFDResultSet extends Closeable {

	/**
	 * Returns starting query position
	 * @return timestamp equal to 'starting' read order
	 */
	public long getStartingPosition();
	
	/**
	 * Returns ending query position
	 * @return timestamp equal to 'ending' read order
	 */
	public long getEndingPosition();
	
	/**
	 * Returns record size
	 * @return record size
	 */
	public int getRecordSize();
	
	/**
	 * Check if there's any data to read
	 * @return if there are no more records to read
	 */
	public boolean isFinished();
	
	/**
	 * Fetches up to bufsize entries into specified buffers
	 * @param timestamps array of size at least bufsize to which timestamps will be written
	 * @param rawdata buffer to which values will be written, in-order as timestamps are. 
	 * Must be at least bufsize*recsize in size
	 * @param bufsize maximum items to read
	 * @return amount of items readed
	 * @throws LFDDamagedException series data was damaged
	 */
	public int fetch(long[] timestamps, ByteBuffer rawdata, int bufsize) throws IOException, LFDDamagedException;
	
	/**
	 * Fetches up to bufsize entries into specified buffers
	 * @param rawdata buffer to which values will be written, in-order as timestamps are.
	 * 		format will be timestamp (8 bytes) in big endian, and then data record
	 * Must be at least bufsize*(8+recsize) in size
	 * @param bufsize maximum items to read
	 * @return amount of items readed
	 * @throws LFDDamagedException series data was damaged
	 */
	public int fetch(ByteBuffer rawdata, int bufsize) throws IOException, LFDDamagedException;
		
	/**
	 * Signals that this result set will not be used anymore
	 */
	public void close() throws IOException;
}
