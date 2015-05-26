package zero.ifc;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.WritableByteChannel;

import zero.store.SeriesDefinition;

/**
 * Generic interface that represents interface of a ZeroDB node.
 * Consumer's don't care if this is a network face or a local node interface.
 * 
 */
public interface SystemInterface extends Closeable {
	
	/**
	 * Change a definition of a given series.
	 * Supplants current definition with this new definition. It will happen only
	 * if generation numbering is right (new gen is larger than current node's gen).
	 * 
	 * This will block until update hits the DB.
	 * 
	 * @param sd SeriesDefinition. CANNOT be null.
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 */
	public void updateDefinition(SeriesDefinition sd) throws LinkBrokenException, IOException;
	
	/**
	 * Obtains a definition of a given series.
	 * 
	 * This attempts to obtain a consensus answer - it may be slow, but it will be sure.
	 * 
	 * @param name name of the series
	 * @return definition of given series, or null if it doesn't exist
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 */
	public SeriesDefinition getDefinition(String seriesname) throws LinkBrokenException, IOException;
	
	/**
	 * Write a value to target series
	 * @param sd descriptor of target series. Must be alive.
	 * @param prev_timestamp timestamp of previous record
	 * @param cur_timestamp timestamp of this record
	 * @param data data to write
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 * @throws IllegalArgumentException timestamps invalid or invalid data
	 * @throws SeriesNotFoundException when a series does not exist (and write was pointless because of that)
	 * @throws DefinitionMismatchException if generation doesn't match (and caller provided an older one)
	 */
	public void writeSeries(SeriesDefinition sd, long prev_timestamp, long cur_timestamp, byte[] data) throws LinkBrokenException, IOException, SeriesNotFoundException, IllegalArgumentException, DefinitionMismatchException;

	/**
	 * Gets target series' operational head (up to this head the series is readable).
	 * Series may contain newer data in it's Write-Ahead, but it may not be readable.
	 * -1 when series is empty.
	 * 
	 * This attempts to obtain a consensus answer - it may be slow, but it will be sure.
	 * 
	 * @param sd descriptor of target series. Must be alive.
	 * @return series' head
	 * @throws LinkBrokenException link to this interface was broken, attempt to reestablish it anew
	 * @throws SeriesNotFoundException when a series does not exist (so it can't be provided)
	 * @throws DefinitionMismatchException if generation doesn't match (and caller provided an older one)
	 */
	public long getHeadTimestamp(SeriesDefinition seriesname) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException;
	
	/**
	 * Gets target series' operational head and included data.
	 * Think of it as a read() that returns only data specified by getHeadTimestamp()
	 * 
	 * Return format is
	 * 	record (8byte big endian TIMESTAMP, binary[recordsize] data) if head != -1
	 * 	8 byte big endian -1 
	 * 
 	 * @param sd descriptor of target series
	 * @param channel target channel
	 */
	public void readHead(SeriesDefinition sd, WritableByteChannel channel) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException;
	
	/**
	 * Read data into target output stream.
	 * 
	 * Format is
	 * 	an array of records (8byte big endian TIMESTAMP, binary[recordsize] data)
	 * 	8 byte big endian -1
	 * 
	 * @param sd descriptor of target series
	 * @param from start timestamp
	 * @param to stop timestamp
	 * @param channel target channel
	 */
	public void read(SeriesDefinition sd, long from, long to, WritableByteChannel channel) throws LinkBrokenException, IOException, SeriesNotFoundException, DefinitionMismatchException, IllegalArgumentException;
}
