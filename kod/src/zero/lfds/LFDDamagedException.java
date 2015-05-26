package zero.lfds;

/**
 * Means the series is (more or less irreparably) damaged
 *
 */
public class LFDDamagedException extends LFDException {
	public LFDDamagedException(String reason) { super(reason); }
}
