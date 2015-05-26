package zero.startup;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.nio.file.Paths;

import zero.lfds.suzie.SUZIEDriver;

import com.google.gson.Gson;


/**
 * Class to return the configuration object
 *
 */
public class ConfigManager {
	
	private static BaseConfiguration instance = null;
	
	/**
	 * Returns the config
	 * @return BaseConfiguration loaded earlier
	 */
	public static BaseConfiguration get() {
		return ConfigManager.instance;
	}
	
	/**
	 * Loads config from a file and stores it for futher retrieval
	 * @param path Path to JSON config file
	 */
	static void loadConfig(String path) {
		Gson gson = new Gson();
		JSONBaseConfiguration bc = null;
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File(path))));
			bc = gson.fromJson(reader, JSONBaseConfiguration.class);
		} catch (FileNotFoundException e) {
			throw new RuntimeException("Configuration file not found");
		}	
		BaseConfiguration b = new BaseConfiguration();
		
		if ((bc.bootstrap_node_ip == null) || (bc.bootstrap_node_port == null))
			b.bootstrap = null;
		else
			b.bootstrap = new InetSocketAddress(bc.bootstrap_node_ip, bc.bootstrap_node_port);

		b.node_interface = new InetSocketAddress(bc.node_ip, bc.node_port);
		
		b.nodehash = bc.nodehash;
		b.series_in_memory = bc.series_in_memory;
		b.gc_grace_period = bc.gc_grace_period;
		b.datapath = Paths.get(bc.seriesdata_path);
		b.metapath = Paths.get(bc.seriesmeta_path);
		b.repair_datapath = Paths.get(bc.seriesdata_repair_path);
		
		b.storage = new SUZIEDriver(b.datapath);
		
		ConfigManager.instance = b;
	}
}
