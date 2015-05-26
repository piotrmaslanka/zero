package zero.store;

import java.io.IOException;

import org.apache.commons.io.FileUtils;

import zero.lfds.LFDAlreadyExistsException;
import zero.lfds.LFDNotFoundException;
import zero.lfds.LFDOpenedException;
import zero.startup.ConfigManager;

/**
 * Class with helpers to change definitions of particular series.
 * 
 * Use it only with unused (fully closed) series.
 * Can be used with non-existent series to define them!
 */
final public class DefinitionUpdateToolkit {
	private SeriesDefinition sd;

	protected DefinitionUpdateToolkit(String seriesName) throws IOException {
		this.sd = SeriesDefinitionDB.getInstance().getSeries(seriesName);
	}
	
	/**
	 * Change to a new definition
	 * @param sd New definition
	 */
	protected void changeTo(SeriesDefinition sd) throws IOException {
		if (this.sd != null) {

			// SPECIAL CASE: If defining with same generation and expired tombstone, data gets physically
			// removed			
			if (this.sd.generation == sd.generation)
				if (sd.tombstonedOn > 0) 
					if (((System.currentTimeMillis() / 1000) - sd.tombstonedOn) > ConfigManager.get().gc_grace_period) {
						// physically trash the data
						SeriesDefinitionDB.getInstance().__hardDeleteSeries(sd.seriesName);
						return;
					}
			
			if (!this.sd.doesSupersede(sd)) return;  // generation not important
		}
		
		boolean performDeletion = false;
		boolean performCreation = false;
		
		if (this.sd == null) {
			performCreation = sd.tombstonedOn == 0;
			performDeletion = false;
		} else {
			// Does current series physical data need to be wiped?
			if (sd.recordSize != this.sd.recordSize) performDeletion = true;
			if (sd.tombstonedOn != 0) performDeletion = true;
			if (sd.options != this.sd.options) performDeletion = true;
			if (sd.replicaCount != this.sd.replicaCount) performDeletion = true;
			
			// Does physical data need to be recreated after?
			if (sd.tombstonedOn == 0) performCreation = true;			
		}
				
		// Apply operations...
		if (performDeletion) {
			try {
				ConfigManager.get().storage.deleteSeries(sd.seriesName);				
				FileUtils.deleteDirectory(ConfigManager.get().repair_datapath.resolve(sd.seriesName).toFile());				
			} catch (LFDNotFoundException e) {				 
			} catch (LFDOpenedException e) {
				throw new RuntimeException("Invariant broken: this should be closed!");
			}			
		}
		
		if (performCreation)
			try {
				ConfigManager.get().storage.createSeries(sd.seriesName, sd.recordSize, sd.options).close();
			} catch (LFDAlreadyExistsException e) {
				throw new RuntimeException("Invariant broken: this does exist!");
			} catch (LFDOpenedException e) {
				// ain't gonna happen
			}
		
		SeriesDefinitionDB.getInstance().updateSeries(sd);
		this.sd = sd;
	}
}
