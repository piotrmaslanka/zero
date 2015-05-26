package zero.lfds.suzie;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Vector;

import org.apache.commons.io.FileUtils;

import zero.lfds.LFDAlreadyExistsException;
import zero.lfds.LFDDamagedException;
import zero.lfds.LFDDriver;
import zero.lfds.LFDNotFoundException;
import zero.lfds.LFDOpenedException;
import zero.lfds.LFDSeries;

/**
 * Rather naive file-based storage.
 * 
 * It doesn't have to be particularly fast or such, it just has to work relatively well.
 * For sure it could be optimized further, but now it's neither the time nor the place
 * to do so.
 * 
 * Following options are accepted ( ; separates )
 * 
 * 		slabsize=10000			-		size of single data file in records
 * 
 * Optimization idea 1: Series should cache their list of data files
 */
public class SUZIEDriver implements LFDDriver {
	protected Path base_directory;
	private HashMap<String, SUZIESeries> series = new HashMap<>();
	private HashMap<String, Integer> series_refcount = new HashMap<>();
	
	public SUZIEDriver(String path) {
		this.base_directory = Paths.get(path);
	}
	
	public SUZIEDriver(Path path) {
		this.base_directory = path;
	}

	/**
	 * Checks whether a series exists, hitting the storage
	 * @param series name
	 * @return whether series exists
	 * @throws IOException upon encountering IOException
	 * @throws LFDDamagedException series is damaged
	 */
	public boolean verifyExistence(String name) throws IOException, LFDDamagedException {
		Path target_directory = this.base_directory.resolve(name);
		
		if (!Files.exists(target_directory)) return false;
		if (!Files.exists(target_directory.resolve("recsize"))) throw new LFDDamagedException("recsize not found");
		if (!Files.exists(target_directory.resolve("options"))) throw new LFDDamagedException("options not found");
		
		return true;
	}
	
	@Override
	public LFDSeries getSeries(String name) throws LFDNotFoundException, LFDDamagedException, IOException { synchronized (this) {
			
		Path target_directory = this.base_directory.resolve(name);
		
		// check for existence in cached maps
		if (series.containsKey(name)) {
			series_refcount.put(name, series_refcount.get(name)+1);
			return series.get(name);
		}
		
		// check whether it exists already
		if (!this.verifyExistence(name)) throw new LFDNotFoundException(); 
		
		// read meta in
		int recsize;
		String options;
		Charset utf8 = Charset.forName("UTF-8");
		
		try {
			recsize = Integer.parseInt(FileUtils.readFileToString(target_directory.resolve("recsize").toFile(), utf8));
		} catch (NumberFormatException e) {
			throw new LFDDamagedException("recsize not an int");
		}
		options = FileUtils.readFileToString(target_directory.resolve("options").toFile(), utf8);		
		
		SUZIESeries s = new SUZIESeries(this, name, recsize, options);
		series.put(name, s);
		series_refcount.put(name, 1);
		return s;
	}}

	@Override
	public LFDSeries createSeries(String name, int recSize, String options)
			throws LFDAlreadyExistsException, IllegalArgumentException, IOException { synchronized (this) {

		// Inspect options
		for (String option : options.split(";")) {
			if (options.length() == 0) continue;
			String kv[] = option.split("=");
			if (kv.length != 2)
				throw new IllegalArgumentException("error in options");
			
			if (kv[0].equals("slabsize"))
				try {
					Long.parseLong(kv[1]);
				} catch (NumberFormatException e) {
					throw new IllegalArgumentException("slabsize value must be an int");
				}
		}
				
		if (this.series.containsKey(name)) throw new LFDAlreadyExistsException();
		// check if a series exists. Kill it if it's damaged
		try {
			if (this.verifyExistence(name)) throw new LFDAlreadyExistsException();
		} catch (LFDDamagedException e) {
			try {
				this.deleteSeries(name);
			} catch (LFDNotFoundException | LFDOpenedException f) {}
		}
		
		Path targetdir = this.base_directory.resolve(name);
		Files.createDirectories(targetdir);
		Charset utf8 = Charset.forName("UTF-8");
		
		FileUtils.write(targetdir.resolve("recsize").toFile(), new Integer(recSize).toString(), utf8);
		FileUtils.write(targetdir.resolve("options").toFile(), options, utf8);
		Files.createFile(targetdir.resolve("0"));
		
		try {
			return this.getSeries(name);
		} catch (LFDDamagedException | LFDNotFoundException e) {
			throw new IOException("damaged after creation");
		}
	}}

	@Override
	public void deleteSeries(String name) throws LFDNotFoundException, LFDOpenedException, IOException { synchronized (this) {
		if (this.series.containsKey(name)) throw new IllegalArgumentException("this series is opened");
		
		FileUtils.deleteDirectory(this.base_directory.resolve(name).toFile());
	}}

	@Override
	public void close() throws LFDOpenedException, IOException {
		this.compactMemory();
		if (this.series_refcount.size() > 0)
			throw new LFDOpenedException();
	}
	
	/**
	 * Called by series to signal that they are being closed
	 * @param name series to signal as closed
	 */
	protected void closeSeries(String name) { synchronized(this) {
		int refs = this.series_refcount.get(name);
		refs--;
		if (refs == 0) {
			this.series.remove(name);
			this.series_refcount.remove(name);
		}
		else 
			this.series_refcount.put(name, refs);
	}}

	@Override
	public void compactMemory() {}
	
	@Override
	public boolean isSeriesNameValid(String name) {
		final char[] ILLEGAL_CHARACTERS = { '/', '\n', '\r', '\t', '\0', '\f', '`', '?', '*', '\\', '<', '>', '|', '\"', ':' };
		for (char ctc : ILLEGAL_CHARACTERS)
			if (name.indexOf(ctc) != -1)
				return false;
		return true;
	}
	
	/**
	 * Returns all series registered in this driver
	 * 
	 * This is SUZIE-specific
	 * 
	 * @return list of series in this driver
	 */
	public String[] enumerateSeries() throws IOException {
		Vector<String> result = new Vector<>();
		DirectoryStream<Path> ds = Files.newDirectoryStream(this.base_directory);
		for (Path path : ds) {
			result.add(path.getFileName().toString());
			System.out.println(path.toString());
		}
		ds.close();
		return result.toArray(new String[0]);
	}

}
