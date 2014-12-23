package be.nabu.libs.datastore.urn;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;

import javax.sql.DataSource;

import be.nabu.libs.resources.URIUtils;

public class DatabaseURNDAO {
	
	private DataSource datasource;
	private TimeZone timezone;

	public DatabaseURNDAO(DataSource datasource, TimeZone timezone) {
		this.datasource = datasource;
		this.timezone = timezone;
	}
	
	/**
	 * There were some problems with an oracle database maintaining the time information in a date field where java.sql.date was used
	 * This normalizes the time component forcibly
	 */
	private Date normalize(Date date) {
		Calendar calendar = Calendar.getInstance(timezone);
		calendar.setTime(date);
		calendar.set(Calendar.HOUR_OF_DAY, 0);
		calendar.set(Calendar.MINUTE, 0);
		calendar.set(Calendar.SECOND, 0);
		calendar.set(Calendar.MILLISECOND, 0);
		return calendar.getTime();
	}
	
	public void map(String id, URI url, URI urn, Date date) throws SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("insert into urns (id, urn, created, url) values (?,?,?,?)");
			int counter = 1;
			statement.setString(counter++, id);
			statement.setString(counter++, urn.toString());
			statement.setDate(counter++, new java.sql.Date(normalize(date).getTime()), Calendar.getInstance(timezone));
			// the method URI.toString() will return escaped values
			// for readability purposes we decode this and encode it again on lookup
			statement.setString(counter++, URIUtils.decodeURI(url.toString()));
			if (statement.executeUpdate() != 1) {
				throw new SQLException("Could not insert urn for " + url);
			}
			commit(connection);
		}
		catch (SQLException e) {
			rollback(connection);
			throw e;
		}
	}
	
	public URI getUrl(URI urn, Date created) throws SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("select url from urns where urn = ? and created = ?");
			int counter = 1;
			statement.setString(counter++, urn.toString());
			statement.setDate(counter++, new java.sql.Date(normalize(created).getTime()), Calendar.getInstance(timezone));
			ResultSet result = statement.executeQuery();
			URI url = null;
			if (result.next()) {
				url = new URI(URIUtils.encodeURI(result.getString("url")));
			}
			commit(connection);
			return url;
		}
		catch (SQLException e) {
			rollback(connection);
			throw e;
		}
		catch (URISyntaxException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
	}
	
	public URI getUrn(URI url) throws SQLException {
		Connection connection = getConnection();
		try {
			PreparedStatement statement = connection.prepareStatement("select urn from urns where url = ?");
			int counter = 1;
			statement.setString(counter++, url.toString());
			ResultSet result = statement.executeQuery();
			URI urn = null;
			if (result.next()) {
				urn = new URI(URIUtils.encodeURI(result.getString("urn")));
			}
			commit(connection);
			return urn;
		}
		catch (SQLException e) {
			rollback(connection);
			throw e;
		}
		catch (URISyntaxException e) {
			rollback(connection);
			throw new RuntimeException(e);
		}
	}
	
	private void rollback(Connection connection) throws SQLException {
		try {
			if (!connection.getAutoCommit() && connection.getTransactionIsolation() != Connection.TRANSACTION_NONE) {
				connection.rollback();
			}
		}
		catch (SQLException e) {
			// ignore
		}
		connection.close();
	}

	private void commit(Connection connection) throws SQLException {
		if (!connection.getAutoCommit() && connection.getTransactionIsolation() != Connection.TRANSACTION_NONE) {
			connection.commit();
		}
		connection.close();
	}
	
	private Connection getConnection() throws SQLException {
		Connection connection = datasource.getConnection();
		connection.setAutoCommit(false);
		return connection;
	}
}
