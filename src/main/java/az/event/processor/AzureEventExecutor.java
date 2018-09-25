package az.event.processor;

import java.io.UnsupportedEncodingException;
import java.sql.SQLException;

import org.json.JSONArray;
import org.json.JSONObject;

import com.microsoft.azure.eventhubs.EventData;

public class AzureEventExecutor {

	public void processBlobCreatedEvent(EventData data) throws SQLException, UnsupportedEncodingException {
		String eventDataString = new String(data.getBytes(), "UTF8");
		JSONArray jsonArray = new JSONArray(eventDataString);

		for (int i = 0; i < jsonArray.length(); i++) {
			JSONObject jsonObject = jsonArray.getJSONObject(i);
			String eventType = jsonObject.getString("eventType");

			if (eventType.equals("Microsoft.Storage.BlobCreated")) {
				JSONObject eventDataJson = jsonObject.getJSONObject("data");

				if (!eventDataJson.getString("contentType").equals("application/octet-stream")) {
					SnowflakeJDBCExample.executeSFCopyInto(eventDataJson.getString("url"));
				}
			}
		}
	}
}
