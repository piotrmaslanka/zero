package zero.lfds;

/**
 * Superclass for all LFD exceptions
 */
public class LFDException extends Exception {
	public LFDException(String reason) {
		super(reason);
	}
	public LFDException() { super(); };
}
