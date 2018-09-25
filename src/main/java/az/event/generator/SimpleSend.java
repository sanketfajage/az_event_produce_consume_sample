package az.event.generator;

import java.io.IOException;
import java.nio.charset.Charset;
import java.time.Instant;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventhubs.EventHubClient;
import com.microsoft.azure.eventhubs.EventHubException;

public class SimpleSend {

	public static void main(String[] args) throws EventHubException, IOException {

		final ConnectionStringBuilder connStr = new ConnectionStringBuilder().setNamespaceName("<name space>")
				.setEventHubName("event hub").setSasKeyName("<policy name>")
				.setSasKey("<SAS key>");

		final Gson gson = new GsonBuilder().create();

		// The Executor handles all asynchronous tasks and this is passed to the
		// EventHubClient instance.
		// The enables the user to segregate their thread pool based on the work
		// load.
		// This pool can then be shared across multiple EventHubClient
		// instances.
		// The following sample uses a single thread executor, as there is only
		// one EventHubClient instance,
		// handling different flavors of ingestion to Event Hubs here.
		final ExecutorService executorService = Executors.newSingleThreadExecutor();

		// Each EventHubClient instance spins up a new TCP/SSL connection, which
		// is expensive.
		// It is always a best practice to reuse these instances. The following
		// sample shows this.
		final EventHubClient ehClient = EventHubClient.createSync(connStr.toString(), executorService);

		try {
			for (int i = 0; i < 100; i++) {
				String payload = "Message " + Integer.toString(i);
				byte[] payloadBytes = gson.toJson(payload).getBytes(Charset.defaultCharset());
				EventData sendEvent = EventData.create(payloadBytes);
				ehClient.sendSync(sendEvent);
			}

			System.out.println(Instant.now() + ": Send Complete...");
			// System.in.read();
		} finally {
			ehClient.closeSync();
			executorService.shutdown();
		}
	}
}
