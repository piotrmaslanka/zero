package zero.ifc;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Vector;

import zero.ifc.LocalInterface;
import zero.ifc.NetworkInterface;
import zero.gossip.NodeDB;

/**
 * You pass it a NodeDB.NodeInfo, it returns you an interface. Fair'n'square
 */
public class InterfaceFactory {
	
	/**
	 * Wrapper to include notion of time when the interface was last used.
	 * Standard timeout is 6 seconds, so we'll just cut it down to 4 seconds to be sure
	 */
	public static class NetworkInterfaceTimed {
		private NetworkInterface ni;
		public long lastUsed;
		
		public NetworkInterfaceTimed(NetworkInterface ni) {
			this.ni = ni;
			this.lastUsed = System.currentTimeMillis();
		}
		
		public boolean isValid() {
			if (!this.ni.isAlive()) return false;
			if ((System.currentTimeMillis() - this.lastUsed) > 4000) return false;
			return true;			
		}
		
		public NetworkInterface getNI() { return this.ni; }
	}
	
	static private Map<Long, Vector<NetworkInterfaceTimed>> pool = new HashMap<>();
	
	static public SystemInterface getInterface(NodeDB.NodeInfo nodeifc, boolean newOne) throws IOException {
		SystemInterface sifc = _getInterface(nodeifc, newOne);
		if (sifc == null)
			return new NetworkInterface(nodeifc.nodecomms, nodeifc);
		return sifc;
	}
	
	/**
	 * Returns an interface. May block
	 * @param nodeifc target node info
	 * @return interface if interface is to be returned, null if needs brand new connection
	 */
	static public synchronized SystemInterface _getInterface(NodeDB.NodeInfo nodeifc, boolean newOne) throws IOException {
		if (nodeifc.isLocal)
			return new LocalInterface();
		else {
			
			// lets try getting it from cache
			Vector<NetworkInterfaceTimed> vni = pool.get(nodeifc.nodehash);
			if (vni == null) { vni = new Vector<>(); pool.put(nodeifc.nodehash, vni); }
			
			if ((vni.size() == 0) || newOne) {
				// spawnve another one
				return null;
				
			} else {
				NetworkInterfaceTimed a = vni.get(0);
				vni.remove(0);
				
				if (!a.isValid()) {
					a.getNI().physicalClose();
					return null;
				}
				
				if (vni.size() == 0) pool.remove(nodeifc.nodehash);
				return a.getNI();
			}
		}
	}
	
	
	static public synchronized void returnConnection(NodeDB.NodeInfo ni, NetworkInterface nifc) throws IOException {
		Vector<NetworkInterfaceTimed> vni = pool.get(ni.nodehash);
		if (vni == null) { vni = new Vector<>(); pool.put(ni.nodehash, vni); }
		vni.add(new NetworkInterfaceTimed(nifc));
	}
	
	/**
	 * This returns a connection with a hint that it has failed
	 */
	static public synchronized void returnConnectionFailed(NodeDB.NodeInfo ni, NetworkInterface nifc) throws IOException {
		nifc.physicalClose();
		// invalidate all connections in this pool!
		
		Vector<NetworkInterfaceTimed> vni = pool.get(ni.nodehash);
		if (vni == null) return;
		
		for (NetworkInterfaceTimed nifct : pool.get(ni.nodehash))
			nifct.getNI().physicalClose();
		
		pool.remove(ni.nodehash);
	}

}
