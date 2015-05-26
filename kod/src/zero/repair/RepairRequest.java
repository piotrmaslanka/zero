package zero.repair;

import zero.store.SeriesDefinition;

public class RepairRequest {
	public SeriesDefinition sd;
	public long from;
	public long to;
	
	public RepairRequest(SeriesDefinition sd, long from, long to) {
		this.sd = sd;
		this.from = from;
		this.to = to;
	}
	
}
