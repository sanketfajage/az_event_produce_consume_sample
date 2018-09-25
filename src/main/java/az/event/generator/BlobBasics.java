package az.event.generator;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.security.InvalidKeyException;
import java.util.Random;

import com.microsoft.azure.storage.StorageException;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;

/**
 * This sample illustrates basic usage of the various Blob Primitives provided
 * in the Storage Client Library including CloudBlobContainer, CloudBlockBlob
 * and CloudBlobClient.
 */
public class BlobBasics {

	public static void main(String[] args) {
		runSamples();
	}

	public static void runSamples() {
		System.out.println("Azure Storage Blob basic sample - Starting.");

		CloudBlobClient blobClient;
		String storageContainerName = "<container name>";

		try {
			// Create a blob client for interacting with the blob service
			blobClient = BlobClientProvider.getBlobClientReference();

			CloudBlobContainer container1 = blobClient.getContainerReference(storageContainerName);

			// Demonstrate block blobs
			System.out.println("\nBasic block blob operations\n");
			basicBlockBlobOperations(container1);
		} catch (Throwable t) {
			PrintHelper.printException(t);
		}
		System.out.println("\nAzure Storage Blob basic sample - Completed.\n");
	}

	/**
	 * Creates and returns a container for the sample application to use.
	 *
	 * @param blobClient
	 *            CloudBlobClient object
	 * @param containerName
	 *            Name of the container to create
	 * @return The newly created CloudBlobContainer object
	 *
	 * @throws StorageException
	 * @throws RuntimeException
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws IllegalArgumentException
	 * @throws InvalidKeyException
	 * @throws IllegalStateException
	 */
	private static CloudBlobContainer createContainer(CloudBlobClient blobClient, String containerName)
			throws StorageException, RuntimeException, IOException, InvalidKeyException, IllegalArgumentException,
			URISyntaxException, IllegalStateException {
		CloudBlobContainer container = blobClient.getContainerReference(containerName);
		try {
			if (!container.createIfNotExists()) {
				throw new IllegalStateException(
						String.format("Container with name \"%s\" already exists.", containerName));
			}
		} catch (StorageException s) {
			if (s.getCause() instanceof java.net.ConnectException) {
				System.out.println(
						"Caught connection exception from the client. If running with the default configuration please make sure you have started the storage emulator.");
			}
			throw s;
		}
		return container;
	}

	/**
	 * Demonstrates the basic operations with a block blob.
	 *
	 * @param container
	 *            The CloudBlobContainer object to work with
	 * @throws IOException
	 * @throws URISyntaxException
	 * @throws StorageException
	 *
	 */
	private static void basicBlockBlobOperations(CloudBlobContainer container)
			throws IOException, URISyntaxException, StorageException {
		try {
			// Create sample files for use
			Random random = new Random();

			System.out.println("\tCreating sample files between 128KB-256KB in size for upload demonstration.");
			File tempFile1 = DataGenerator.createTempLocalFile("blockblob-", ".tmp",
					(128 * 1024) + random.nextInt(128 * 1024));
			System.out.println(String.format("\t\tSuccessfully created the file \"%s\".", tempFile1.getAbsolutePath()));

			// Upload a sample file as a block blob
			System.out.println("\n\tUpload a sample file as a block blob.");
			CloudBlockBlob blockBlob1 = container.getBlockBlobReference("blockblobtest.tmp");
			blockBlob1.uploadFromFile(tempFile1.getAbsolutePath());
			tempFile1.deleteOnExit();
			System.out.println("\t\tSuccessfully uploaded the blob.");
		} catch (StorageException s) {
			if (s.getErrorCode().equals("BlobTypeNotSupported")) {
				System.out.println(String.format("\t\tError: %s", s.getMessage()));
			} else {
				throw s;
			}
		}
	}
}
