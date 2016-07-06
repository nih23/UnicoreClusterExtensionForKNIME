package org.tudresden.unicore.knime.filehandling;

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.Connection;
import org.tudresden.unicore.UnicoreConnector;
import org.tudresden.unicore.UnicoreRESTConnector;

/**
 * this class encapsulates (active) unicore connections
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */

public class UnicoreConnection extends Connection {

	UnicoreConnector m_unicore_connector;
	
	public UnicoreConnection(final URI uri, String gateway, String queue, final ConnectionInformation connectionInformation) {
		String username = connectionInformation.getUser();
		String pass = connectionInformation.getPassword();
		// filter site and storage from uri
		m_unicore_connector = new UnicoreRESTConnector(username, pass, gateway);
		m_unicore_connector.setSitename(queue);
	}
	
	
	@Override
	public void open() throws Exception {
		m_unicore_connector.openConnection();
		
	}

	@Override
	public boolean isOpen() {
		return true;
	}

	@Override
	public void close() throws Exception {
		m_unicore_connector.closeConnection();
	}
	
	public UnicoreConnector getConnector() {
		return m_unicore_connector;
	}

}
