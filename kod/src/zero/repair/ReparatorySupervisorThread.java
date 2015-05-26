package zero.repair;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ConcurrentLinkedDeque;

import zero.startup.ConfigManager;
import zero.store.NotFoundException;
import zero.store.SeriesController;
import zero.store.SeriesDB;
import zero.store.SeriesDefinitionDB;

/**
 * Thread that 
 * 	1) Get repair requests
 *  2) 
 */
public class ReparatorySupervisorThread extends Thread {

	static private ReparatorySupervisorThread instance;
	private volatile boolean isTerminated = false;
	
	public void terminate() {
		this.isTerminated = true;
	}	
	
	public static ReparatorySupervisorThread getInstance() {
		if (instance == null) instance = new ReparatorySupervisorThread();
		return instance;
	}
	
	private ConcurrentLinkedDeque<RepairRequest> requests = new ConcurrentLinkedDeque<>();
	
	public void postRequest(RepairRequest rr) {
		this.requests.addFirst(rr);
	}
	
	public ReparatorySupervisorThread() {
		super();
	}
	
	private RepairRequest get_repair_request_for(String seriesname) throws IOException {
		// ok, dump all of that stuff now
		long minimum = Long.MAX_VALUE;		

		try (DirectoryStream<Path> ds = Files.newDirectoryStream(ConfigManager.get().repair_datapath.resolve(seriesname))) {
			for (Path path : ds) {
				try {
					long fnam = Long.parseLong(path.getFileName().toString());
					if (fnam < minimum) minimum = fnam;
				} catch (NumberFormatException p) {}
			}
		}

		
		try (SeriesController sercon = SeriesDB.getInstance().getSeries(seriesname)) {
			long head = sercon.getHeadTimestamp();
			
			if (head > minimum)
				// What the fuck
				throw new IOException();

			return new RepairRequest(sercon.getSeriesDefinition(), head, minimum);
		} catch (NotFoundException e) {
			throw new IOException();
		}		
	}
	

	public void run() {
		String last_repaired = null;
		
		System.out.println("REPAIR: Repair supervisor thread starting");
		int consequent_nothings = 0;
		while (!isTerminated) {
			try {
				
				RepairRequest rr = this.requests.pollLast();
				if (rr == null) {
					Thread.sleep(4000);
					consequent_nothings++;

					if (consequent_nothings == 6) {
						// Full rescan, try to hunt some partials
						try (DirectoryStream<Path> stream = Files.newDirectoryStream(ConfigManager.get().repair_datapath)) {
							for (Path entry : stream) {
								this.requests.push(this.get_repair_request_for(entry.getFileName().toString()));
								break;
							}
						}
					}
				} else {
					if (rr.sd.seriesName.equals(last_repaired)) {
						Thread.sleep(2000);
						last_repaired = null;
						continue;
					}
					last_repaired = rr.sd.seriesName;
					// does it make sense?
					SeriesController sercon = null;
					try {
						sercon = SeriesDB.getInstance().getSeries(rr.sd.seriesName);
						if (!sercon.doesPartialExist(rr.to)) {
							sercon.close();
							continue;
						}
					} catch (NotFoundException e) {
						sercon.close();
						continue;
					}
					ReparatoryThread rt = new ReparatoryThread(sercon, rr.from, rr.to);
					rt.start();
					rt.join();
				}
			} catch (IOException | InterruptedException e) {
				consequent_nothings = 0;
			}
		}
	}
	
}
