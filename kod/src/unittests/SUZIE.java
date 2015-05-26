package unittests;

import static org.junit.Assert.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import zero.lfds.LFDDriver;
import zero.lfds.LFDResultSet;
import zero.lfds.LFDSeries;
import zero.lfds.suzie.SUZIEDriver;

/**
 * A general LFD conformance test. Use all LFDs you might want there!
 */
public class SUZIE {
	private LFDDriver driver;
	
	@Before
	public void setUp() throws Exception {
		try {
			Files.createDirectory(Paths.get("test"));
		} catch (IOException e) {
		}
		this.driver = new SUZIEDriver("test");
	}

	@After
	public void tearDown() throws Exception {
		this.driver.close();
	}

	@Test
	public void basicSeriesReadout2() throws Exception {
		LFDSeries ser = this.driver.createSeries("basicSeriesReadout", 4, "");
		byte[] data = new byte[] { 1,  2, 3, 4};
		ser.write(0, data);
		ser.write(1, data);
		
		// do some preliminary readouts
		int rows_readed_in = 0;
		long[] tses = new long[1];
		ByteBuffer dats = ByteBuffer.allocate(4);

		// Read 1:
		LFDResultSet rs = ser.read(1, 1);
		assertEquals(rs.isFinished(), false);		
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rs.isFinished(), true);			
		assertEquals(rows_readed_in, 1);
		rows_readed_in = 0;
		rs.close();

		// Read 1:
		rs = ser.read(1, 1);
		ByteBuffer xdats = ByteBuffer.allocate((8+4)*1024);
		assertEquals(rs.isFinished(), false);		
		while (true) {
			int rrin = rs.fetch(xdats, 1);
			xdats.clear();
			if (rrin == 0) break;
			rows_readed_in += rrin;
		}
		assertEquals(rs.isFinished(), true);			
		assertEquals(rows_readed_in, 1);
		rows_readed_in = 0;
		rs.close();		
		
		// close series
		ser.close();
		this.driver.deleteSeries("basicSeriesReadout");
	}
	
	@Test
	public void basicSeriesReadout1() throws Exception {
		LFDSeries ser = this.driver.createSeries("basicSeriesReadout", 4, "");
		byte[] data = new byte[] { 1,  2, 3, 4};
		ser.write(1, data);
		
		// do some preliminary readouts
		int rows_readed_in = 0;
		long[] tses = new long[1];
		ByteBuffer dats = ByteBuffer.allocate(4);

		// Read 1:
		LFDResultSet rs = ser.read(1, 2);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 1);
		rows_readed_in = 0;
		rs.close();

		// Read 2:
		rs = ser.read(3, 4);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 0);
		rows_readed_in = 0;
		rs.close();

		// Read 3:
		rs = ser.read(0, 0);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 0);
		rows_readed_in = 0;
		rs.close();
	
		// Read 4:
		rs = ser.read(1, 1);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 1);
		rows_readed_in = 0;
		rs.close();		
		
		// close series
		ser.close();
		this.driver.deleteSeries("basicSeriesReadout");
	}
	
	
	@Test
	public void basicSeriesReadout4() throws Exception {
		LFDSeries ser = this.driver.createSeries("basicSeriesReadout", 4, "");
		byte[] data = new byte[] { 1, 2, 3, 4};
		ser.write(2, data);
		ser.write(10, data);
		ser.write(15, data);
		ser.write(20, data);
		
		// do some preliminary readouts
		int rows_readed_in = 0;
		long[] tses = new long[1];
		ByteBuffer dats = ByteBuffer.allocate(4);
		
		// Read 1:
		LFDResultSet rs = ser.read(0, Long.MAX_VALUE);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 4);
		rows_readed_in = 0;
		rs.close();

		// Read 2:
		rs = ser.read(2, Long.MAX_VALUE);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 4);
		rows_readed_in = 0;
		rs.close();
		
		// Read 3:
		rs = ser.read(2, 20);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 4);
		rows_readed_in = 0;
		rs.close();
		
		// Read 4:
		rs = ser.read(0, 19);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 3);
		rows_readed_in = 0;
		rs.close();
		
		// Read 5:
		rs = ser.read(30, 40);
		while (rs.fetch(tses, dats, 1) == 1) { dats.clear(); rows_readed_in++; }
		assertEquals(rows_readed_in, 0);
		rows_readed_in = 0;
		rs.close();		
	
	
		// close series
		ser.close();
		this.driver.deleteSeries("basicSeriesReadout");
	}
}
