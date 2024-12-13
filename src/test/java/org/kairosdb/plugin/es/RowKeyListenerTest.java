package org.kairosdb.plugin.es;

import junit.framework.TestCase;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.kairosdb.events.RowKeyEvent;

import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;

public class RowKeyListenerTest extends TestCase
{

	public void testRowKeyEvent() throws InterruptedException
	{
		RowKeyListener rowKeyListener = new RowKeyListener("192.168.1.216", 9200);

		SortedMap<String, String> tags = new TreeMap<>();
		tags.put("tag1", "value1");
		tags.put("tag2", "value2");
		RowKeyEvent rowKeyEvent = new RowKeyEvent("new_metric", new DataPointsRowKey("new_metric", "one", System.currentTimeMillis(), "long", tags), 0);
		rowKeyListener.rowKeyEvent(rowKeyEvent);

		Thread.sleep(5000);
	}
}