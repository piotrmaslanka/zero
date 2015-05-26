package zero.store;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.Serializable;

/**
 * A particular series definition
 *
 */
public class SeriesDefinition implements Serializable {
	private static final long serialVersionUID = 1L;

	/**
	 * Name of the series
	 */
	public String seriesName;
	
	/**
	 * Amount of replicas in the system
	 */
	public int replicaCount;
	
	/**
	 * Identifier of the series generation. If a generation
	 * changes, that means a series was deleted and remade new
	 */
	public long generation;	
	
	
	/**
	 * Upon a write, a trim() with argument of write_timestamp - autoTrim will be issued.
	 * Set to 0 to disable this functionality
	 */
	public long autoTrim;
	
	/**
	 * Size of a record
	 */
	public int recordSize;

	/**
	 * Options passed to underlying storage
	 */
	public String options;
	
	/**
	 * Timestamp at which the series was deleted.
	 * 
	 * 0 if this has not yet been deleted
	 */
	public long tombstonedOn;
	
	public SeriesDefinition(String name, int recordSize, int replicaCount, String options) {
		this.seriesName = name;
		this.recordSize = recordSize;
		this.replicaCount = replicaCount;
		this.options = options;
		this.tombstonedOn = 0;
		this.generation = 0;
		this.autoTrim = 0;
	}
	 
	public boolean equals(SeriesDefinition sd) {
		if (sd == null) return false;
		return this.generation == sd.generation;
	}
	
	/**
	 * Checks whether sd is more important than this definition
	 * by comparing generation
	 * @param sd a definition that is supposed to be more important than this one
	 * @return whether sd is more current/important than this definition
	 */
	public boolean doesSupersede(SeriesDefinition sd) {
		if (sd == null) return false;
		return sd.generation > this.generation;
	}

	
	// ------------------------------------ INTP representation
	public void toDataStreamasINTPRepresentation(DataOutputStream dos) throws IOException {
		dos.writeInt(replicaCount);
		dos.writeInt(recordSize);
		dos.writeLong(generation);
		dos.writeLong(autoTrim);
		dos.writeLong(tombstonedOn);
		dos.writeUTF(options);
		dos.writeUTF(seriesName);
	}
	
	public static SeriesDefinition fromDataStreamasINTPRepresentation(DataInputStream dis) throws IOException {
		int replicaCount = dis.readInt();
		int recordSize = dis.readInt();
		long generation = dis.readLong();
		long autotrim = dis.readLong();
		long tombstonedon = dis.readLong();
		String options = dis.readUTF();
		String seriesName = dis.readUTF();
		
		SeriesDefinition nd = new SeriesDefinition(seriesName, recordSize, replicaCount, options);
		nd.autoTrim = autotrim;
		nd.tombstonedOn = tombstonedon;
		nd.generation = generation;
		return nd;
	}	
}
