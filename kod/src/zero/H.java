package zero;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * The principal hashing function
 *
 */
public class H {
	
	/**
	 * Hashes the name.
	 * @param name Name to hash
	 * @param replica_no Number of replica, counted from 0
	 * @return Hash value
	 */
	public static long hash(String name, int replica_no) {
		long basehash = H.h_hash(name);
		if (replica_no == 0)
			return basehash;
		if (replica_no == 1)
			return basehash + Long.MAX_VALUE;
		if (replica_no == 2)
			return basehash + (Long.MAX_VALUE) + (Long.MAX_VALUE / 2);
		if (replica_no == 3)
			return basehash + (Long.MAX_VALUE / 2);
		throw new RuntimeException("No idea how to hash that");
	}
	
	private static long h_hash(String name) {
		try {
			MessageDigest md = MessageDigest.getInstance("SHA-1");
			md.update(name.getBytes(Charset.forName("UTF-8")));
			byte[] digest = md.digest();
			
			ByteBuffer bb = ByteBuffer.allocate(digest.length);
			bb.put(digest);
			bb.flip();
			return bb.getLong();
			
		} catch (NoSuchAlgorithmException e) {
			throw new RuntimeException("No SHA-1 on this platform!");
		}
		
		
	}
}
