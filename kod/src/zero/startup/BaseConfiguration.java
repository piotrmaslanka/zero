package zero.startup;

import java.net.InetSocketAddress;
import java.nio.file.Path;

import zero.lfds.LFDDriver;

/**
 * Base configuration info object, after being parsed from JSON
 * and filled in by ConfigManager
 *
 */
public class BaseConfiguration {
	/**
	 * bootstrap node address. null if seed node
	 */
	public InetSocketAddress bootstrap;

	/**
	 * this node's interface
	 */
	public InetSocketAddress node_interface;
	
	/**
	 * Desired hash of this node
	 */
	public long nodehash;
	
	/**
	 * Directory in which time series data should be stored
	 */
	public Path datapath;
	
	/**
	 * Directory in which time series metadata should be stored
	 */
	public Path metapath;
	
	/**
	 * Directory in which parts of damaged time series will be stored
	*/
	public Path repair_datapath;
	
	
	public int gc_grace_period;
	
	public int series_in_memory;
	
	// -------------------- derived from configuration
	
	/**
	 * Storage for principal data
	 */
	public LFDDriver storage;

}