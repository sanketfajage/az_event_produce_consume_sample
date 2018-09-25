package az.event.processor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.microsoft.azure.eventprocessorhost.ExceptionReceivedEventArgs;

/*
 * The general notification handler is an object that derives from
 * Consumer<> and takes an ExceptionReceivedEventArgs object as an argument.
 * The argument provides the details of the error: the exception that
 * occurred and the action (what EventProcessorHost was doing) during which
 * the error occurred. The complete list of actions can be found in
 * EventProcessorHostActionStrings.
 */
public class ErrorNotificationHandler implements Consumer<ExceptionReceivedEventArgs> {

	private static final Logger LOG = LoggerFactory.getLogger(ErrorNotificationHandler.class);

	@Override
	public void accept(ExceptionReceivedEventArgs t) {
		StringWriter stack = new StringWriter();
		t.getException().printStackTrace(new PrintWriter(stack));
		LOG.error("Host {} received general error notification during {}. Exception : {}", t.getHostname(),
				t.getAction(), stack);
	}
}
