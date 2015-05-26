package zero.lfds;

import java.io.IOException;

/**
 * Every LFD driver must implement this.
 * 
 * Particular path passed to constructor will uniquely identify given driver.
 * Driver must be threadsafe. It is up to programmer to provide how. Implementation guarantees
 * that only one object with given path will be alive at one time.
 */
public interface LFDDriver {
	/**
	 * Returns an access object for given series.

	 * At minimum, it must be checked that given series exists
	 * 
	 * Please note that even that this can throw LFDDamagedException, it does not need to verify integrity
	 * with 100% assurance! Series can be successfully opened though some of it is still damaged.
	 * 
	 * @param path path identifying database
	 * @return object representing series requested
	 * @throws LFDNotFoundException when particular series was not found
	 * @throws LFDDamagedException when data series was found damaged
	 */
	public LFDSeries getSeries(String name) throws LFDNotFoundException, LFDDamagedException, IOException;
	
	/**
	 * Creates given series.
	 * 
	 * If a series exists, but is damaged, it will be overwritten
	 * 
	 * @param name series name
	 * @param recSize target record size
	 * @param options options in driver-specific format
	 * @return object representing created series
	 * @throws LFDAlreadyExistsException when series already exists
	 * @throws IllegalArgumentException when options are invalid
	 */
	public LFDSeries createSeries(String name, int recSize, String options) throws LFDAlreadyExistsException, IllegalArgumentException, IOException;
	
	/**
	 * Deletes target series
	 * 
	 * Should reliably delete series even when it's damaged (precluding of course massive storage failure, which
	 * should be reported by IOException)
	 * 
	 * @param name name of series to delete
	 * @throw LFDNotFoundException when series does not exist
	 * @throw LFDOpenedException when this series is currently opened
	 */
	public void deleteSeries(String name) throws LFDNotFoundException, LFDOpenedException, IOException;

	/**
	 * Closes the driver. This means that no operations will be used after this.
	 * 
	 * You can safely associate this with closing the database connection.
	 * @throws LFDOpenedException when not all series were closed
	 */
	public void close() throws LFDOpenedException, IOException;
	
	/**
	 * Called by controller if working memory is tight and the driver should attempt to release some
	 */
	public void compactMemory() throws IOException;
	
	/**
	 * Checks whether given series name is valid.
	 * 
	 * Validity does not mean or imply existence!
	 * 
	 * @param name Name to check for validity
	 * @return whether given name is valid
	 */
	public boolean isSeriesNameValid(String name);

}
