package az.event.processor;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SnowflakeJDBCExample {

	private static final Logger LOG = LoggerFactory.getLogger(SnowflakeJDBCExample.class);

	static Connection connection = ConnectionFactory.getSnowflakeConnection();

	private SnowflakeJDBCExample() {
	}

	public static void executeSFCopyInto(String fileUrl) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			fileUrl = fileUrl.replace("https", "azure");

			String stageName = "AZURE_BLOB_STORAGE";
			createSFStage(stageName, fileUrl);
			copyFromSFStage(stageName);

			copyWithoutSFStage(fileUrl);
			showResultFromSFTable();
		}
	}

	private static void createSFStage(String stageName, String fileUrl) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			// create an external stage
			LOG.info("Creating stage with name {}", stageName);
			String createStageStmt = "create or replace stage " + stageName + " url=\'" + fileUrl + "\'"
					+ " credentials=(azure_sas_token=" + AzureConstants.SAS_TOKEN + ")";
			statement.executeUpdate(createStageStmt);
			LOG.info("Stage created");
		}
	}

	private static void copyFromSFStage(String stageName) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			LOG.info("Copying into table from stage {}", stageName);
			String copyIntoStmt = "COPY INTO table_name FROM @" + stageName;
			statement.executeUpdate(copyIntoStmt);
			LOG.info("Copy completed");
		}
	}

	private static void copyWithoutSFStage(String fileUrl) throws SQLException {
		try (Statement statement = connection.createStatement()) {
			LOG.info("Copying into table without stage");
			String copyIntoStmt = "COPY INTO table_name FROM \'" + fileUrl + "\'" + " credentials=(azure_sas_token="
					+ AzureConstants.SAS_TOKEN + ")";
			statement.executeUpdate(copyIntoStmt);
			LOG.info("Copy completed");
		}
	}

	private static void showResultFromSFTable() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String selectStmt = "select * from table_name";
			LOG.info("Metadata:");
			LOG.info("================================");

			// fetch metadata
			try (ResultSet resultSet = statement.executeQuery(selectStmt)) {
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				LOG.info("Number of columns={}", resultSetMetaData.getColumnCount());
				for (int colIdx = 0; colIdx < resultSetMetaData.getColumnCount(); colIdx++) {
					LOG.info("Column {}: type={}", colIdx, resultSetMetaData.getColumnTypeName(colIdx + 1));
				}

				// fetch data
				LOG.info("\nData:");
				LOG.info("================================");
				int rowIdx = 0;
				while (resultSet.next()) {
					LOG.info("row {}, column 0: {}, column 1: {}", rowIdx, resultSet.getString(1),
							resultSet.getString(2));
					System.out.println("row " + rowIdx + ", column 0: " + resultSet.getString(1) + ", column 1: "
							+ resultSet.getString(2));
					rowIdx++;
				}
			}
		}
	}

	public void insertTest() throws SQLException {
		try (Statement statement = connection.createStatement()) {
			String copyIntoStmt = "insert into <db_name>.<schema>.<table_name> (<col>) values (<value>)";
			statement.executeUpdate(copyIntoStmt);
			LOG.info("Copy completed");
		}
	}
}
