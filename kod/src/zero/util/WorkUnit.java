package zero.util;

/**
 * Unit of work.
 * 
 * Pass any arguments it needs in constructor.
 * 
 * Invoke run() when you transition onto it, or run it as a thread if you want to
 */
abstract public class WorkUnit {
	
	public abstract void run() throws Exception;
	
	public void executeAsThread() {
		WorkUnit.executeAsThread(this);
	}
	
	public static void executeAsThread(final WorkUnit u) {
		new Thread(
				new Runnable() {
					@Override
					public void run() {
						try {
							u.run();
						} catch (Exception exc) {
							
						}
					}
				}).start();
	}
}
