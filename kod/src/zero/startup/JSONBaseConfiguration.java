package zero.startup;

/**
 * This class has it's contents readed from a .json configuration file.
 * 
 * This is a global config class. 
 */
public class JSONBaseConfiguration {
	
	/** Address of nearby node to initiate the bootstrap protocol. Leave blank if this is a seed node */
	public String bootstrap_node_ip;
	/** Port of nearby node to initiate the bootstrap protocol. Leave blank if this is a seed node */
	public Integer bootstrap_node_port;

	
	/** Address of this node's interface */
	public String node_ip;
	/** Port of this node's interface */
	public Integer node_port;
	
	
	/**
	 * Desired hash of this node
	 */
	public long nodehash;
	
	
	/**
	 * Directory in which series data will be stored
	 */
	public String seriesdata_path;
	
	/**
	 * Directory in which series metadata will be stored
	 */
	public String seriesmeta_path;
	
	
	/**
	 * Directory in which parital series will be stored,
	 * for those series that await a repair.
	 * It will be filled with LFD drivers, each for a 
	 * series that has to be repaired
	 */
	public String seriesdata_repair_path;
	
	
	/**
	 * Amount of seconds, which need to pass from
	 * tombstoning a series to it's metadata removal
	 */
	public int gc_grace_period;
	
	/**
	 * Amount of series to keep in memory at once
	 */
	public int series_in_memory;
	
}
