package zero.gossip;

import java.util.HashMap;


/**
 * Singleton class used for detecting and enumerating failures within the system
 */
public class FailureDetector {
	private static FailureDetector instance;
	
	public static FailureDetector getInstance() {
		if (FailureDetector.instance == null) FailureDetector.instance = new FailureDetector();
		return FailureDetector.instance;
	} 
	
	private HashMap<Long, Integer> watchlist = new HashMap<>();	// node hash => failure count
	
	/**
	 * Called upon a failure to communicate with target node
	 * @param nodehash hash of node that communication was attempted to
	 */
	public synchronized void onFailure(final long nodehash) {
		Integer cntr = this.watchlist.get(nodehash);
		if (cntr == null) {
			this.watchlist.put(nodehash, 1);
			return;
		} else {
			this.watchlist.put(nodehash, cntr+1);
		}
		
		if (cntr > 2)
			NodeDB.getInstance().onAlivenessChange(nodehash, false);
	}
	
	/**
	 * Called upon a success to communicate with target node
	 * @param nodehash hash of node that communication with succeeded
	 */
	public synchronized void onSuccess(long nodehash) {
		this.watchlist.remove(nodehash);
		NodeDB.getInstance().onAlivenessChange(nodehash, true);
	}
	
}
