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