package zero.gcollector;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;

import zero.startup.ConfigManager;
import zero.store.SeriesDB;
import zero.store.SeriesDefinition;
import zero.store.SeriesDefinitionDB;

/**
 * Thread that reaps tombstoned metadata that expired
 *
 */
public class GarbageCollectionThread extends Thread {

	public void run() {
		System.out.println("GC: Garbage collection thread starting");
		
		while (true) {
			try {
				Thread.sleep(ConfigManager.get().gc_grace_period);
			} catch (InterruptedException e1) {}
			
			long now = System.currentTimeMillis() / 1000;
			
			try (DirectoryStream<Path> stream = Files.newDirectoryStream(ConfigManager.get().metapath)) {
				for (Path entry: stream) {
					String name = entry.getFileName().toString();
					
					SeriesDefinition sd = SeriesDefinitionDB.getInstance().getSeries(name);
					if (sd == null) continue;
					
					if (sd.tombstonedOn != 0)
						if ((now - sd.tombstonedOn) > ConfigManager.get().gc_grace_period)
							// we should remove it HARD
							// DefinitionUpdateToolkit has a fabulous function - if you try 
							// to update with same generation AND expired tombstoned
							// it will physically remove the data from disk
							SeriesDB.getInstance().redefineAsync(sd);
				}
				
			} catch (IOException e) {}
		}
	}
}
