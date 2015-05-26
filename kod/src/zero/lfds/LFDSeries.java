package zero.lfds;

import java.io.IOException;

/**
 * Objects representing particular time series must implement this.
 * 
 * Series must be threadsafe. It is up to programmer to provide how.
 *
 */
public interface LFDSeries {
	/**
	 * Returns record size for given series
	 * @return record size for given series
	 */
	public int getRecordSize();
	
	/**
	 * Returns series name
	 * @return series name
	 */
	public String getName();
	
	
	/**
	 * Reads specified data from the series
	 * 
	 * from must be less or equal than to
	 * 
	 * Data will be returned with 'from' record included (if it exists), but without 'to' (even if it exists).
	 * 'to' simply specifies bound.
	 * 
	 * @return a result set that can be used to retrieve the data
	 * @param from to start reading, inclusive
	 * @param to to end reading, inclusive
	 */
	public LFDResultSet read(long from, long to) throws IOException, LFDDamagedException;
	
	/**
	 * Reads entire series
	 * @return a result set representing the entire LFD
	 */
	public LFDResultSet readAll() throws IOException, LFDDamagedException;
	
	/**
	 * Returns head timestamp (leading timestamp).
	 * 
	 * This should be a relatively cheap operation
	 * 
	 * @return head, or -1 if empty
	 */
	public long getHeadTimestamp();
	
	/**
	 * Writes a next position into the series
	 * @param timestamp Timestamp of write
	 * @param data Data to write
	 * @throws InvalidArgumentException on invalid length of data or timestamp less or equal than head
	 */
	public void write(long timestamp, byte[] data) throws IllegalArgumentException, IOException;
	
	/**
	 * Signals that this object will no longer be used
	 * @throws LFDOpenedException when not all result sets were closed
	 */
	public void close() throws IOException, LFDOpenedException;
	
	/**
	 * Called by controller if working memory is tight and the series should attempt to release some
	 */
	public void compactMemory() throws IOException;	
	
	/**
	 * Called by controller if syncing this series to storage is requested
	 */
	public void sync() throws IOException;
	
	/**
	 * Called to signal that given data is not needed anymore and can be removed
	 * 
	 * It's just a hint. Data-to-be-removed can still be returned, but it's at LFD's discretion.
	 * This can be called at any time, even when result sets are open. As LFD can choose a time to 
	 * perform the deletion, this is not relevant.
	 * 
	 * @param split data with timestamp less than this is free to be removed
	 * @throws IOException if a I/O error has occurred.
	 */
	public void trim(long split) throws IOException;
}
