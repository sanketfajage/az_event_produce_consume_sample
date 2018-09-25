package az.event.processor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.ConnectionStringBuilder;
import com.microsoft.azure.eventprocessorhost.EventProcessorHost;
import com.microsoft.azure.eventprocessorhost.EventProcessorOptions;

public class AzureEventHubSubcriber {

	private static final Logger LOG = LoggerFactory.getLogger(AzureEventHubSubcriber.class);

	public void processAzureEvents() throws InterruptedException, ExecutionException {

		/*
		 * To conveniently construct the Event Hub connection string from the
		 * raw information, use the ConnectionStringBuilder class.
		 */
		ConnectionStringBuilder eventHubConnectionString = new ConnectionStringBuilder()
				.setNamespaceName(AzureConstants.NAMESPACE).setEventHubName(AzureConstants.EVENT_HUB)
				.setSasKeyName(AzureConstants.SAS_KEY_NAME).setSasKey(AzureConstants.SAS_KEY);

		/*
		 * Create the instance of EventProcessorHost using the most basic
		 * constructor. This constructor uses Azure Storage for persisting
		 * partition leases and checkpoints. The host name, which identifies the
		 * instance of EventProcessorHost, must be unique. You can use a plain
		 * UUID, or use the createHostName utility method which appends a UUID
		 * to a supplied string.
		 */
		EventProcessorHost host = new EventProcessorHost(EventProcessorHost.createHostName(AzureConstants.HOST_PREFIX),
				AzureConstants.EVENT_HUB, AzureConstants.CONSUMER_GROUP, eventHubConnectionString.toString(),
				AzureConstants.STORAGE_CONTAINER_ENDPOINT, AzureConstants.CONTAINER);

		/*
		 * Registering an event processor class with an instance of
		 * EventProcessorHost starts event processing. The host instance obtains
		 * leases on some partitions of the Event Hub, possibly stealing some
		 * from other host instances, in a way that converges on an even
		 * distribution of partitions across all host instances. For each leased
		 * partition, the host instance creates an instance of the provided
		 * event processor class, then receives events from that partition and
		 * passes them to the event processor instance.
		 */

		/*
		 * There are two error notification systems in EventProcessorHost.
		 * Notification of errors tied to a particular partition, such as a
		 * receiver failing, are delivered to the event processor instance for
		 * that partition via the onError method. Notification of errors not
		 * tied to a particular partition, such as initialization failures, are
		 * delivered to a general notification handler that is specified via an
		 * EventProcessorOptions object. You are not required to provide such a
		 * notification handler, but if you don't, then you may not know that
		 * certain errors have occurred.
		 */
		EventProcessorOptions options = new EventProcessorOptions();
		options.setExceptionNotification(new ErrorNotificationHandler());

		host.registerEventProcessor(EventProcessor.class, options).whenComplete((unused, e) -> {
			/*
			 * whenComplete passes the result of the previous stage through
			 * unchanged, which makes it useful for logging a result without
			 * side effects.
			 */
			if (e != null) {
				StringWriter stack = new StringWriter();
				e.printStackTrace(new PrintWriter(stack));
				LOG.error("Failure while registering: {}", stack);
			}
		}).thenAccept(unused -> {
			/*
			 * This stage will only execute if registerEventProcessor succeeded.
			 * If it completed exceptionally, this stage will be skipped.
			 */
			try {
				System.in.read();
			} catch (Exception e) {
				StringWriter stack = new StringWriter();
				e.printStackTrace(new PrintWriter(stack));
				LOG.error("Keyboard read failed: {}", stack);
			}
		}).thenCompose(unused ->
		// This stage will only execute if registerEventProcessor succeeded.

		/*
		 * Processing of events continues until unregisterEventProcessor is
		 * called. Unregistering shuts down the receivers on all currently owned
		 * leases, shuts down the instances of the event processor class, and
		 * releases the leases for other instances of EventProcessorHost to
		 * claim.
		 */
		host.unregisterEventProcessor()).exceptionally(e -> {
			StringWriter stack = new StringWriter();
			e.printStackTrace(new PrintWriter(stack));
			LOG.error("Failure while un-registering: {}", stack);
			return null;
		}).get();
	}
}
