package org.tudresden.unicore.knime.filehandling;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.core.node.NodeLogger;
/**
 * container for connection parameter
 * @author Nico Hoffmann
 *
 */
public class UnicoreConnectionInformation extends ConnectionInformation implements Serializable {
    
	/**
	 * 
	 */
	private static final long serialVersionUID = 12312345546L;
	String m_defaultPath = "";
	String m_storageSink = "";
	String m_gateway = "";
	String m_queue = "";
	
	public void setStorageSink(String storageSink) {
		m_storageSink = storageSink;
	}
	
	public String getStorageSink() {
		return m_storageSink;
	}
	
	public void setDefaultPath(String defaultPath) {
		m_defaultPath = defaultPath;
	}
	
	public String getDefaultPath() {
		return m_defaultPath;
	}
	
	public String getGateway() {
		return m_gateway;
	}
	
	public String getQueue() {
		return m_queue;
	}
	
	public void setQueue(String q) {
		m_queue = q;
	}
	
	public void setGateway(String gw) {
		m_gateway = gw;
	}
	
	/**
     * Create the corresponding uri to this connection information.
     *
     *
     * @return URI to this connection information
     */
    public URI toURI() {
        URI uri = null;
        try {
            uri = new URI(super.getProtocol(), super.getUser(), super.getHost(), super.getPort(), "/storages/" + getStorageSink() + "/files" + getDefaultPath(), null, null);
        } catch (final URISyntaxException e) {
            // Should not happen
            NodeLogger.getLogger(getClass()).coding(e.getMessage(), e);
        }
        return uri;
    }

}
