package org.kairosdb.plugin.es;

import co.elastic.clients.elasticsearch.ElasticsearchAsyncClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.http.HttpHost;
import org.elasticsearch.client.RestClient;
import org.kairosdb.datastore.cassandra.DataPointsRowKey;
import org.kairosdb.eventbus.Subscribe;
import org.kairosdb.events.RowKeyEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.SortedMap;

import static org.kairosdb.datastore.cassandra.DataPointsRowKeySerializer.UTF8;

public class RowKeyListener
{
	Logger s_logger = LoggerFactory.getLogger(RowKeyListener.class);

	public static final String METRIC_NAME = "metric_name";
	public static final String DATA_TYPE = "data_type";
	public static final String TIMESTAMP = "timestamp";
	public static final String TAG_PREFIX = "tag_";

	public static final String HTTP_HOST_PROP = "kairosdb.es.http_host";
	public static final String PORT_PROP = "kairosdb.es.port";
	public static final String INDEX_PROP = "kairosdb.es.index";

	private static SimpleDateFormat s_dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
	private final ElasticsearchAsyncClient m_client;

	private String m_index = "kairos_keys";

	@Inject
	public void setIndex(@Named(INDEX_PROP)String index)
	{
		m_index = index;
	}

	@Inject
	public RowKeyListener(@Named(HTTP_HOST_PROP) String hostname, @Named(PORT_PROP) int port)
	{
		// Create the low-level client
		RestClient restClient = RestClient.builder(
				new HttpHost(hostname, port)).build();

		// Create the transport with a Jackson mapper
		ElasticsearchTransport transport = new RestClientTransport(
				restClient, new JacksonJsonpMapper());

		// And create the API client
		m_client = new ElasticsearchAsyncClient(transport);
	}

	/**
	 Each document in solr needs an id.  Because our documents are all unique
	 we will use a hash of each field to create and id.
	 @param rowKey
	 @return
	 */
	private String hashRowKey(DataPointsRowKey rowKey)
	{
		MessageDigest messageDigest = null;
		try
		{
			messageDigest = MessageDigest.getInstance("MD5");
		}
		catch (NoSuchAlgorithmException e) {}

		messageDigest.update(rowKey.getMetricName().getBytes(UTF8));
		messageDigest.update(rowKey.getDataType().getBytes(UTF8));

		//Hash timestamp
		long timestamp = rowKey.getTimestamp();
		for (int I = 7; I >= 0; --I)
		{
			messageDigest.update((byte) (timestamp & 0xff));
			timestamp >>= 8;
		}

		//Hash tags
		SortedMap<String, String> tags = rowKey.getTags();
		for (String tagName : tags.keySet())
		{
			messageDigest.update(tagName.getBytes(UTF8));
			messageDigest.update(tags.get(tagName).getBytes(UTF8));
		}

		byte[] digest = messageDigest.digest();

		return new BigInteger(1, digest).toString(16);
	}

	@Subscribe
	public void rowKeyEvent(RowKeyEvent event)
	{
		DataPointsRowKey rowKey = event.getRowKey();
		String keyHash = hashRowKey(rowKey);

		Map<String, String> dataMap = new HashMap<>();
		dataMap.put(METRIC_NAME, event.getMetricName());
		dataMap.put(DATA_TYPE, rowKey.getDataType());
		dataMap.put(TIMESTAMP, s_dateFormat.format(new Date(rowKey.getTimestamp())));

		SortedMap<String, String> tags = rowKey.getTags();
		for (String tagKey : tags.keySet())
		{
			dataMap.put(TAG_PREFIX+tagKey, tags.get(tagKey));
		}

		m_client.index(i ->
				i.index(m_index)
						.id(keyHash)
						.document(dataMap))
				.whenComplete((response, exception) -> {
			if (exception != null) {
				s_logger.error("Failed to index", exception);
			} else {
				s_logger.info("Indexed with version " + response.version());
			}
		});

	}
}
