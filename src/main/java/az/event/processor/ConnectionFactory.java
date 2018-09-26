package az.event.processor;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConnectionFactory {

	private static final Logger LOG = LoggerFactory.getLogger(ConnectionFactory.class);

	private ConnectionFactory() {
	}

	public static Connection getSnowflakeConnection() {
		try {
			Properties properties = buildConnectionProperties();
			String connectionUrl = "<connection_url>";
			return DriverManager.getConnection(connectionUrl, properties);
		} catch (SQLException e) {
			StringWriter stack = new StringWriter();
			e.printStackTrace(new PrintWriter(stack));
			LOG.error(stack.toString());
		}
		return null;
	}

	private static Properties buildConnectionProperties() {
		Properties properties = new Properties();
		properties.put("user", SnowflakeConstants.USERNAME);
		properties.put("password", SnowflakeConstants.PASSWORD);
		properties.put("account", SnowflakeConstants.ACCOUNT_NAME);
		properties.put("warehouse", SnowflakeConstants.WAREHOUSE);
		properties.put("db", SnowflakeConstants.DB_NAME);
		properties.put("schema", SnowflakeConstants.SCHEMA_TYPE);
		return properties;
	}
}
