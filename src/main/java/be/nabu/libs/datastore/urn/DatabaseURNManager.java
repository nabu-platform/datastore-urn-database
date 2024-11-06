/*
* Copyright (C) 2014 Alexander Verbruggen
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU Lesser General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
* GNU Lesser General Public License for more details.
*
* You should have received a copy of the GNU Lesser General Public License
* along with this program. If not, see <https://www.gnu.org/licenses/>.
*/

package be.nabu.libs.datastore.urn;

import java.net.URI;
import java.net.URISyntaxException;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;

import javax.sql.DataSource;

import be.nabu.libs.datastore.api.URNManager;

public class DatabaseURNManager implements URNManager {

	private String domain;
	
	private static ThreadLocal<SimpleDateFormat> formatter = new ThreadLocal<SimpleDateFormat>();

	private DatabaseURNDAO dao;

	private TimeZone timezone;
	
	public DatabaseURNManager(DataSource dataSource, TimeZone timezone, String domain) {
		if (domain == null) {
			throw new NullPointerException("The domain can not be null");
		}
		else if (domain.contains(":")) {
			throw new IllegalArgumentException("The domain can not contain ':'");
		}
		this.domain = domain;
		this.timezone = timezone;
		this.dao = new DatabaseURNDAO(dataSource, timezone);
	}
	
	@Override
	public URI map(URI url) {
		Date date = new Date();
		String id = UUID.randomUUID().toString();
		URI urn = generateUrn(url, date, id);
		try {
			dao.map(id, url, urn, date);
			return urn;
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public URI resolve(URI urn) {
		try {
			return dao.getUrl(urn, extractDate(urn));
		}
		catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected URI generateUrn(URI url, Date date, String id) {
		SimpleDateFormat formatter = getFormatter();
		try {
			return new URI("urn:" + domain + ":" + formatter.format(date) + "-" + id);
		}
		catch (URISyntaxException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected String extractId(URI urn) {
		return urn.getSchemeSpecificPart().replaceAll("[^:]+:[0-9]{4}/[0-9]{2}/[0-9]{2}-(.*)", "$1");
	}
	
	protected Date extractDate(URI urn) {
		SimpleDateFormat formatter = getFormatter();
		String date = urn.getSchemeSpecificPart().replaceAll("[^:]+:([0-9]{4}/[0-9]{2}/[0-9]{2})-.*", "$1");
		try {
			return formatter.parse(date);
		}
		catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}
	
	protected SimpleDateFormat getFormatter() {
		if (formatter.get() == null) {
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy/MM/dd");
			simpleDateFormat.setTimeZone(timezone);
			formatter.set(simpleDateFormat);
		}
		return formatter.get();
	}
}
