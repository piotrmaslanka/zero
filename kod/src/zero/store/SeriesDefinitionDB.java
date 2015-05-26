package zero.store;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import zero.startup.ConfigManager;

/**
 * Source of information about defined series.
 * Singleton.
 * 
 * Not threadsafe.
 *
 */
public class SeriesDefinitionDB {
	private Path db_root;		// Path to the meta database
	
	private SeriesDefinitionDB() {
		this.db_root = ConfigManager.get().metapath;
	}
	
	
	/**
	 * Deletes a series definition.
	 * Essentially increments generation and tombstones it.
	 * @param sname series name
	 */
	public void deleteSeries(String seriesname) throws IOException {
		SeriesDefinition sd = this.getSeries(seriesname);
		sd.generation++;
		sd.tombstonedOn = System.currentTimeMillis();
		this.updateSeries(sd);
	}

	
	/**
	 * Deletes series definition data from disk
	 * @param seriesname series name
	 */
	public void __hardDeleteSeries(String seriesname) throws IOException {
		Files.delete(this.db_root.resolve(seriesname));
	}
	
	/**
	 * Updates a series definition.
	 * @param sd New SD file to serialize in the database
	 */
	public void updateSeries(SeriesDefinition sd) throws IOException {
		FileOutputStream fos = new FileOutputStream(this.db_root.resolve(sd.seriesName).toString());
		ObjectOutputStream oos = new ObjectOutputStream(fos);
		oos.writeObject(sd);
		oos.close();
	}
	
	/**
	 * Returns information about a series of particular name
	 * @param name Series name to retrieve
	 * @return series definition, or null if information was not found
	 */
	public SeriesDefinition getSeries(String name) throws IOException {
		Path fdef = this.db_root.resolve(name);
		
		// does the metadata exist?
		if (!Files.exists(fdef)) return null;
		
		// load the metadata
		FileInputStream fis = new FileInputStream(fdef.toString());
		ObjectInputStream ois = new ObjectInputStream(fis);
		try {
			SeriesDefinition sid = (SeriesDefinition)ois.readObject();
			return sid;
		} catch (ClassNotFoundException e) {
			throw new IOException("class not found. File damaged??");
		} finally {
			ois.close();
		}
	}

	// ---------------- singletonish code
	private static SeriesDefinitionDB instance = null;	
	public static SeriesDefinitionDB getInstance() {
		if (SeriesDefinitionDB.instance == null) SeriesDefinitionDB.instance = new SeriesDefinitionDB();
		return SeriesDefinitionDB.instance;
	}
}
