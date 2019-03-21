package org.tudresden.unicore.knime.filehandling;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.net.URI;
import java.util.List;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.core.node.NodeLogger;
import org.tudresden.unicore.UnicoreConnector;
import org.tudresden.unicore.knime.UnicoreExecutorHandler;


/**
 * representation of a file located on remote sites being accessible to unicore
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreRemoteFile extends RemoteFile<UnicoreConnection> implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 7916606384957075529L;
	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreExecutorHandler.class);
	URI m_uri;
	String m_storage_sink;
	String m_gateway;
	String m_queue;
	String m_directory;
	Boolean m_isDirectory;
	
	Boolean m_integratedViaStaging;
	
	//protected UnicoreRemoteFile(URI uri, String gateway, String storageSink, String queue, ConnectionInformation connectionInformation,
	protected UnicoreRemoteFile(URI uri, ConnectionInformation connectionInformation,
			ConnectionMonitor<UnicoreConnection> connectionMonitor) {
		super(uri, connectionInformation, connectionMonitor);
		
		if(!(connectionInformation instanceof UnicoreConnectionInformation)) {
			LOGGER.error("Unicore Remote File was initialized with wrong connection information!");
			return;
		}
		
		UnicoreConnectionInformation uci = (UnicoreConnectionInformation)connectionInformation;
		
		m_uri = uri;
		m_storage_sink = uci.getStorageSink();
		m_queue = uci.getQueue();
		m_gateway = uci.getGateway();
		m_isDirectory = false;
		String pUri = m_uri.getPath();
		
		if(m_uri.toString().endsWith("/") || pUri.equals("")) {
			m_isDirectory = true;
		}/* else {
			m_isDirectory = 
		}*/
		
		m_integratedViaStaging = false;
	}

	public void integrateViaStaging(Boolean value) {
		m_integratedViaStaging = value;
	}
	
	public Boolean isIntegratedViaStaging() {
		return m_integratedViaStaging;
	}
	
	@Override
	protected boolean usesConnection() {
		return true;
	}

	@Override
	protected UnicoreConnection createConnection() {
		
		UnicoreConnection unicoreConn = super.getConnectionMonitor().findConnection("globalConn"); 
		if(unicoreConn == null) {
			unicoreConn = new UnicoreConnection(m_uri, m_gateway, m_queue, super.getConnectionInformation());
			super.getConnectionMonitor().registerConnection("globalConn", unicoreConn);
		}
		return unicoreConn;
	}

	@Override
	public String getType() {
		return "u6";
	}

	@Override
	public boolean exists() throws Exception {
		return true;
	}

	/**
	 * the URI encodes complete information for REST api, yet the REST connector only requires us to provide the relative directory.. 
	 * @return
	 */
	private String getPathFromURI() {
		String uriPath = m_uri.getPath();
		String remotePath = uriPath.replace("/storages/" + m_storage_sink + "/files", "");
		return remotePath;
	}
	
	public String getURIForDataStaging() {
		// the following only works for ucc for now (15042016)
		// String stagingURI = "u6://" + getConnection().getConnector().getSite() + "/" + m_storage_sink + getPathFromURI(); 

		// REST url
		//String stagingURI = getConnection().getConnector().getSite() + "/rest/core/storages/" + m_storage_sink + "/files/" +  getPathFromURI();
		
		// WSRF-URL
		String stagingURI = m_queue + "/services/StorageManagement?res=" + m_storage_sink + "#/" +  getPathFromURI();
		
		stagingURI = stagingURI.replace("//", "/");

		if(!m_gateway.startsWith("https://")) {
			stagingURI = "https://" + m_gateway + stagingURI;
		} else {
			stagingURI = m_gateway + stagingURI;
		}
		
		stagingURI = "BFT:" + stagingURI;
		
		return stagingURI;
				
	}
	
	@Override
	public boolean isDirectory() throws Exception {
		return m_isDirectory;
		/*
		try {
			if(getConnection() == null) {
				open();
			}
			return getConnection().getConnector().isDirectory(m_storage_sink, getPathFromURI());
		} catch (Exception e) {
			System.out.println(e.getMessage());
			if(getConnection() == null) {
				open();
			}
			getConnection().getConnector().isDirectory(m_storage_sink, getPathFromURI());
		}
		return false;*/
	}
	
	public boolean isDirectoryRemote() throws Exception {
		try {
			if(getConnection() == null) {
				open();
			}
			return getConnection().getConnector().isDirectory(m_storage_sink, getPathFromURI());
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
			if(getConnection() == null) {
				open();
			}
			getConnection().getConnector().isDirectory(m_storage_sink, getPathFromURI());
		}
		return false;
	}

	@Override
	public InputStream openInputStream() throws Exception {
		return new ByteArrayInputStream(getConnection().getConnector().downloadFile(m_storage_sink, getPathFromURI()));		
	}

	// upload not implemented yet
	@Override
	public OutputStream openOutputStream() throws Exception {
		//byte[] file = getConnection().getConnector().downloadFile(m_storage_sink, getPathFromURI());	
		//return null;
		throw new Exception();
	}

	@Override
	public long getSize() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long lastModified() throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public boolean delete() throws Exception {
		throw new Exception();
		//return false;
	}

	private UnicoreConnection checkAndGetConnection() {
		if(getConnection() == null) {
			try {
				open();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		return getConnection();
	}
	
	@Override
	public RemoteFile<UnicoreConnection>[] listFiles() throws Exception {
		System.out.println("List files -> " + m_uri.getPath());
		List<String> listing = checkAndGetConnection().getConnector().listFiles(m_storage_sink, getPathFromURI());
		RemoteFile<UnicoreConnection>[] dirListing = new UnicoreRemoteFile[listing.size()]; 
		for(int i = 0 ; i < listing.size(); i++) {
			// TODO: fix bug with double storage sink name
			dirListing[i] = new UnicoreRemoteFile(URI.create(getIdentifier() + listing.get(i)), getConnectionInformation(), getConnectionMonitor());
		}
		
		return dirListing;
	}

	@Override
	public boolean mkDir() throws Exception {
		throw new Exception();
		//return false;
	}
	/*
	 * returns local path on remote system
	 */
	public String getRemoteLocalPath() throws Exception {
		if(isIntegratedViaStaging()) {
			String stagingDir = System.getenv("UNICORE_STAGING_DIRECTORY");

			if(stagingDir == null)
				stagingDir = "";
			return stagingDir + "/stagingData/" + getName();
		}
			
		
		String cachedPath = UnicoreConnector.getCachedInformationForStorageSink(m_storage_sink);
		
		if(cachedPath == null)
			return checkAndGetConnection().getConnector().getLocalPathOfRemoteFile(m_storage_sink, getPathFromURI());
		
		String res = cachedPath + getPathFromURI();
		
		return res;
	}
	
	public void setStorageSink(String storage_sink) {
		m_storage_sink = storage_sink;
	}

}
