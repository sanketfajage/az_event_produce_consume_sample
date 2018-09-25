package az.event.processor;

import java.io.PrintWriter;
import java.io.StringWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventhubs.EventData;
import com.microsoft.azure.eventprocessorhost.CloseReason;
import com.microsoft.azure.eventprocessorhost.IEventProcessor;
import com.microsoft.azure.eventprocessorhost.PartitionContext;

public class EventProcessor implements IEventProcessor {

	private static final Logger LOG = LoggerFactory.getLogger(EventProcessor.class);

	private int checkpointBatchingCount = 0;

	/*
	 * OnOpen is called when a new event processor instance is created by the
	 * host. In a real implementation, this is the place to do initialization so
	 * that events can be processed when they arrive, such as opening a database
	 * connection.
	 */
	@Override
	public void onOpen(PartitionContext context) throws Exception {
		LOG.info("SAMPLE: Partition {} is opening", context.getPartitionId());
	}

	/*
	 * OnClose is called when an event processor instance is being shut down.
	 * The reason argument indicates whether the shut down is because another
	 * host has stolen the lease for this partition or due to error or host
	 * shutdown. In a real implementation, this is the place to do cleanup for
	 * resources that were opened in onOpen.
	 */
	@Override
	public void onClose(PartitionContext context, CloseReason reason) throws Exception {
		LOG.info("SAMPLE: Partition {} is closing for reason {}", context.getPartitionId(), reason);
	}

	/*
	 * onError is called when an error occurs in EventProcessorHost code that is
	 * tied to this partition, such as a receiver failure. It is NOT called for
	 * exceptions thrown out of onOpen/onClose/onEvents. EventProcessorHost is
	 * responsible for recovering from the error, if possible, or shutting the
	 * event processor down if not, in which case there will be a call to
	 * onClose. The notification provided to onError is primarily informational.
	 */
	@Override
	public void onError(PartitionContext context, Throwable error) {
		LOG.error("SAMPLE: Partition {} onError: {}", context.getPartitionId(), error);
	}

	/*
	 * onEvents is called when events are received on this partition of the
	 * Event Hub. The maximum number of events in a batch can be controlled via
	 * EventProcessorOptions. Also, if the "invoke processor after receive
	 * timeout" option is set to true, this method will be called with null when
	 * a receive timeout occurs.
	 */
	@Override
	public void onEvents(PartitionContext context, Iterable<EventData> events) throws Exception {
		LOG.info("SAMPLE: Partition {} got event batch", context.getPartitionId());
		AzureEventExecutor azureEventProcessor = new AzureEventExecutor();
		int eventCount = 0;
		for (EventData data : events) {
			try {
				azureEventProcessor.processBlobCreatedEvent(data);
			} catch (Exception exception) {
				StringWriter stack = new StringWriter();
				exception.printStackTrace(new PrintWriter(stack));
				LOG.error(stack.toString());
			}

			/*
			 * It is important to have a try-catch around the processing of each
			 * event. Throwing out of onEvents deprives you of the chance to
			 * process any remaining events in the batch.
			 */
			try {
				eventCount++;
				/*
				 * Checkpointing persists the current position in the event
				 * stream for this partition and means that the next time any
				 * host opens an event processor on this event hub+consumer
				 * group+partition combination, it will start receiving at the
				 * event after this one. Checkpointing is usually not a fast
				 * operation, so there is a tradeoff between checkpointing
				 * frequently (to minimize the number of events that will be
				 * reprocessed after a crash, or if the partition lease is
				 * stolen) and checkpointing infrequently (to reduce the impact
				 * on event processing performance). Checkpointing every five
				 * events is an arbitrary choice for this sample.
				 */
				this.checkpointBatchingCount++;
				if ((checkpointBatchingCount % 5) == 0) {
					LOG.info("SAMPLE: Partition {} checkpointing at {}, {}", context.getPartitionId(),
							data.getSystemProperties().getOffset(), data.getSystemProperties().getSequenceNumber());
					/*
					 * Checkpoints are created asynchronously. It is important
					 * to wait for the result of check pointing before exiting
					 * onEvents or before creating the next checkpoint, to
					 * detect errors and to ensure proper ordering.
					 */
					context.checkpoint(data).get();
				}
			} catch (Exception e) {
				StringWriter stack = new StringWriter();
				e.printStackTrace(new PrintWriter(stack));
				LOG.error("Processing failed for an event: {}", stack);
			}
		}
		LOG.info("SAMPLE: Partition {} batch size was {} for host {}", context.getPartitionId(), eventCount,
				context.getOwner());
	}
}
