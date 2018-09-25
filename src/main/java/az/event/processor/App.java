package az.event.processor;

import java.util.concurrent.ExecutionException;

public class App {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		AzureEventHubSubcriber azureEventHubSubcriber = new AzureEventHubSubcriber();
		azureEventHubSubcriber.processAzureEvents();
	}
}
