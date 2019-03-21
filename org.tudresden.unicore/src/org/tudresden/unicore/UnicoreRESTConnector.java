package org.tudresden.unicore;

import java.io.File;
import java.io.FileInputStream;
import java.io.StringReader;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.security.cert.X509Certificate;
import java.util.LinkedList;
import java.util.List;

import javax.json.Json;
import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonReader;
import javax.json.JsonString;
import javax.json.JsonValue;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;

import org.knime.cluster.util.JobStatus;

/**
 * REST connector to unicore/unity remote server 
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreRESTConnector implements UnicoreConnector {
	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreRESTConnector.class);
	private Client m_activeClient;

	String m_username;
	String m_password;
	String m_gateway;
	String m_site;
	String m_storageSinkId;


	SSLContext m_sslContext;
	TrustManager[] m_trustManagers_trustAllCerts;
	HttpAuthenticationFeature m_basicAuthFeature;
	ClientBuilder m_unicoreRestClientBuilder;


	public UnicoreRESTConnector(String username, String password, String gateway) {
		m_username = username;
		m_password = password;
		m_gateway = gateway;
		m_site = null;
	}

	/*public void setStorageSinkId(String storageSinkId) {
	    m_storageSinkId = storageSinkId;
	}

	public String getStorageSinkId() {
	    return m_storageSinkId;
	}*/

	public void setSitename(String site) {
		m_site = site;
	}

	public String getSite() {
		return m_site;
	}

	public LinkedList<String> getAvailiableSites() {

		return null;
	}
	




	/********************************************************
	 * functions from unicore connector interface
	 ********************************************************/



	/**
	 * {@inheritDoc}
	 */
	@Override
	public LinkedList<String> getAvailiableStorageSites() throws Exception {
		WebTarget target = getClient().target(getUnicoreBaseURI());
		Response resp = target.path("storages").request(MediaType.APPLICATION_JSON).get();
		LOGGER.info("storages " + resp.getStatus() + ": " + resp.getStatusInfo());

		javax.json.JsonReader reader = Json.createReader(new StringReader(resp.readEntity(String.class)));
		JsonArray availStoragesRaw = reader.readObject().getJsonArray("storages");
		LinkedList<String> availStorages  = new LinkedList<String>();
		for(JsonValue singleStoreRaw : availStoragesRaw) {
			String singleStore = singleStoreRaw.toString().replace("\"", "");
			String strgId = singleStore.substring(singleStore.lastIndexOf("/")+1); // remove REST URI from storage id
			availStorages.add(strgId);
		}
		return availStorages;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public JsonObject getInformationForSingleStorageSink(String storageSinkId) throws Exception {
		if(m_storageSinkInformationCache.containsKey(storageSinkId)) {
			return m_storageSinkInformationCache.get(storageSinkId);
		}

		WebTarget target = getClient().target(getUnicoreBaseURI());		
		Response resp = target.path("storages").path(storageSinkId).request(MediaType.APPLICATION_JSON).get();
		String data = resp.readEntity(String.class);
		LOGGER.info("storages_info " + resp.getStatus() + ": " + resp.getStatusInfo());
		JsonReader reader = Json.createReader(new StringReader(data));
		JsonObject infoS1 = reader.readObject();

		m_storageSinkInformationCache.put(storageSinkId, infoS1);

		return infoS1;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public boolean uploadFile(File fileToUpload, String storageSinkId, String remotePath) throws Exception {
		//API   /storages/{id}/files/{filePath} 	PUT 	application/octet-stream
		//boolean exists = fileToUpload.exists();
		//long length = fileToUpload.length();
		//DataInputStream dis = new DataInputStream(new FileInputStream(fileToUpload));    
		LOGGER.info("uploading " + fileToUpload.getName() + " via unicore REST interface");
		WebTarget target = getClient().target(getUnicoreBaseURI()).path("storages").path(storageSinkId)
				.path("files")
				//.path(remotePath)
				.path(fileToUpload.getName());
		Response resp = target.request(MediaType.APPLICATION_OCTET_STREAM)
				.put(Entity.entity(new FileInputStream(fileToUpload), MediaType.APPLICATION_OCTET_STREAM));
		LOGGER.info("file upload completed " + resp.getStatus() + ": " + resp.getStatusInfo());

		return resp.getStatus() == 204;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public byte[] downloadFile(String storageSinkId, String remotePath) throws Exception {
		//API   /storages/{id}/files/{filePath} 	GET 	application/octet-stream  
		LOGGER.info("downloading " + remotePath + " via unicore REST interface");
		WebTarget target = getClient().target(getUnicoreBaseURI()).path("storages").path(storageSinkId)
				.path("files")
				.path(remotePath);
		Response resp = target.request(MediaType.APPLICATION_OCTET_STREAM).get();
		byte[] downloadedObject = resp.readEntity(byte[].class);

		if(resp.getStatus() == 404) {
			// LOGGER.warn("Couldn't find remote file " + remotePath);
		}

		LOGGER.info("file download completed " + resp.getStatus() + ": " + resp.getStatusInfo());

		return downloadedObject;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public String submitJob(JsonObject jobDescription) throws Exception {
		// submit job
		WebTarget target = getClient().target(getUnicoreBaseURI()).path("jobs");
		String jobDesc = jobDescription.toString();
		Response resp = target.request(MediaType.APPLICATION_JSON).post(Entity.entity(jobDescription.toString(), MediaType.APPLICATION_JSON));
		LOGGER.info("job submission " + resp.getStatus() + ": " + resp.getStatusInfo());

		if(resp.getStatus() != 201) {
			LOGGER.error("Error while submitting job: " + resp.getStatusInfo());
			return null;
		}

		// retrieve id of our job
		String jobId = "";
		jobId = resp.getHeaderString("Location");
		jobId = jobId.substring(jobId.lastIndexOf("/")+1);


		return jobId;
	}

	/**
	 * {@inheritDoc}
	 * @throws Exception 
	 */
	@Override
	public JobStatus getStateOfJob(String jobId) throws Exception {
		WebTarget target = getClient().target(getUnicoreBaseURI());
		Response resp = target.path("jobs").path(jobId).request(MediaType.APPLICATION_JSON).get();
		LOGGER.info("jobs " + resp.getStatus() + ": " + resp.getStatusInfo());
		if(resp.getStatus() == 500) {
			LOGGER.error("Received internal Server error while updating state of job " + jobId + " with Unicore server.");
		}
		javax.json.JsonReader reader = Json.createReader(new StringReader(resp.readEntity(String.class)));

		JsonObject jobProperties = reader.readObject();

		//String dbg = jobProperties.toString();

		// get job state from json
		String jobStatus = jobProperties.getString("status");
		LOGGER.info("Status of " + jobId + " is " + jobStatus);
		switch (jobStatus) {
		case "SUCCESSFUL":
			return JobStatus.FINISHED;
		case "STAGINGIN":
			return JobStatus.RUNNING;
		case "STAGINGOUT":
			return JobStatus.RUNNING;
		case "RUNNING":
			return JobStatus.RUNNING;
		case "READY":
			return JobStatus.RUNNING;
		case "QUEUED":
			return JobStatus.QUEUED;
		case "UNDEFINED":
			//TODO: how should we realize proper handling of this state?
			return JobStatus.UNKNOWN;
		case "FAILED":
			JsonArray log = jobProperties.getJsonArray("log");
			JsonString lst = log.getJsonString(log.size()-1);
			LOGGER.error(lst);
			return JobStatus.FAILED;
		}

		JsonArray log = jobProperties.getJsonArray("log");
		JsonString lst = log.getJsonString(log.size()-1);
		LOGGER.error(lst);
		return JobStatus.UNKNOWN;
	}

	public void startJob(String jobId, String jobDescription) throws Exception {
		WebTarget target = getClient().target(getUnicoreBaseURI());
		Response resp = target.path("jobs").path(jobId).path("actions").path("start").request().post(Entity.entity(jobDescription.toString(), MediaType.APPLICATION_JSON));
		
		// Entity.entity(jobDescription.toString(), MediaType.APPLICATION_JSON)
		
		LOGGER.info("starting job " + resp.getStatus() + ": " + resp.getStatusInfo());
		
	}
	
	@Override
	public void destroyJob(String jobId) throws Exception {
		WebTarget target = getClient().target(getUnicoreBaseURI());
		Response resp = target.path("jobs").path(jobId).request().delete();
		LOGGER.info("destroy job " + resp.getStatus() + ": " + resp.getStatusInfo());
		closeConnection();
	}

	public List<String> getListOfAllJobIDs() throws Exception {
		LinkedList<String> jobIds = new LinkedList<String>();
		WebTarget target = getClient().target(getUnicoreBaseURI());
		Response resp = target.path("jobs").request(MediaType.APPLICATION_JSON).get();
		LOGGER.info("list of all jobs " + resp.getStatus() + ": " + resp.getStatusInfo());
		if(resp.getStatus() == 500) {
			LOGGER.error("Received internal Server error while retrieving list of all jobs of Unicore server.");
		}
		javax.json.JsonReader reader = Json.createReader(new StringReader(resp.readEntity(String.class)));

		JsonObject jobProperties = reader.readObject();
		JsonArray jobs = jobProperties.getJsonArray("jobs");

		for(JsonValue job : jobs) {
			String id = job.toString().substring(job.toString().lastIndexOf("/")+1);
			id = id.substring(0,id.length()-1);
			jobIds.add(id);
		}

		return jobIds;
	}

	/********************************************************
	 * PRIVATE FUNCTIONS
	 ********************************************************/

	/**
	 * prepare necessary components to open ssl-secured authenticated connection to unicore/unity server
	 * @throws NoSuchAlgorithmException
	 * @throws KeyManagementException
	 */
	private void initRestAuthenticationStuff() throws NoSuchAlgorithmException, KeyManagementException {
		/*
		 * workaround for self-signed certificates 
		 */	
		// Create a trust manager that does not validate certificate chains
		m_trustManagers_trustAllCerts = new TrustManager[]{new X509TrustManager(){
			public X509Certificate[] getAcceptedIssuers(){return null;}
			public void checkClientTrusted(X509Certificate[] certs, String authType){}
			public void checkServerTrusted(X509Certificate[] certs, String authType){}
		}};

		// Install the all-trusting "trust" manager
		m_sslContext = SSLContext.getInstance("TLS");
		m_sslContext.init(null, m_trustManagers_trustAllCerts, new SecureRandom());


		// init RESTful client and authenticate to server
		m_basicAuthFeature = HttpAuthenticationFeature.basicBuilder()
				.credentials(m_username,m_password)		  // non-preemptive auth.
				.build();
		m_unicoreRestClientBuilder = ClientBuilder.newBuilder().register(m_basicAuthFeature).sslContext(m_sslContext);
	}

	public void closeConnection() throws Exception {
		m_activeClient.close();
	}

	public synchronized void openConnection() throws Exception {
		if(m_sslContext == null) {
			try {
				initRestAuthenticationStuff();
			} catch (KeyManagementException e) {
				LOGGER.warn(e.getMessage(), e);
			} catch (NoSuchAlgorithmException e) {
				LOGGER.warn(e.getMessage(), e);
			}
		}

		if(m_activeClient == null) {
			m_activeClient = m_unicoreRestClientBuilder.build();
		}

		try {
			// test connection		
			WebTarget target = m_activeClient.target(getUnicoreBaseURI());
			Response resp = target.request(MediaType.APPLICATION_JSON).get();
			// catch exception due to closed connection and open connection
			LOGGER.info("auth " + resp.getStatus() + ": " + resp.getStatusInfo());
		} catch (IllegalStateException e) {
			LOGGER.warn(e.getMessage(), e);
			m_activeClient = m_unicoreRestClientBuilder.build();
			WebTarget target = m_activeClient.target(getUnicoreBaseURI());
			Response resp = target.request(MediaType.APPLICATION_JSON).get();
		}
		

	}

	private String getUnicoreBaseURI() throws InvalidSettingsException {
		if(m_gateway == null || m_site == null) {
			throw new InvalidSettingsException("gateway or site not set");
		}

		return new StringBuilder(m_gateway)
				.append(m_site)
				.append("/rest/core").toString();	
	}

/*	private String getUnicoreStorageBaseURI() throws InvalidSettingsException {
		if(m_site == null) {
			throw new InvalidSettingsException("site not set");
		}

		return new StringBuilder(m_gateway)
				.append("/rest/core").toString();	
	}
*/

	private Client getClient() {
		//TODO: check if connection is open
		/*if(m_activeClient == null) {*/
		try {
			openConnection();
		} catch (Exception e) {
			LOGGER.error("Can't open REST connection to Unicore gateway!", e);
		}

		return m_activeClient;
	}

	@Override
	public List<String> listFiles(String storageSinkId, String directory) throws Exception {
		// /storages/{id}/files/{filePath}
		LinkedList<String> availStorages = new LinkedList<String>();
		if(directory.equals("")) {
			directory = "/";
		}
		WebTarget target = getClient().target(getUnicoreBaseURI()).path("storages").path(storageSinkId).path("files").path(directory);
		Response resp = target.request(MediaType.APPLICATION_JSON).get();
		//LOGGER.setLevel(LEVEL.ALL);
		LOGGER.info("storages " + resp.getStatus() + ": " + resp.getStatusInfo());
		if(resp.getStatus() == 404) {
			LOGGER.warn("Warning path " + directory + " not found.");
			return availStorages;
		}
		javax.json.JsonReader reader = Json.createReader(new StringReader(resp.readEntity(String.class)));



		JsonObject obj = reader.readObject();
		JsonArray availStoragesRaw = obj.getJsonArray("children");
		for(JsonValue singleStoreRaw : availStoragesRaw) {
			//String singleStore = singleStoreRaw.toString().replace("\"", "");
			//String strgId = singleStore.substring(singleStore.lastIndexOf("/")+1); // remove REST URI from storage id
			String rawStr = singleStoreRaw.toString();
			rawStr = rawStr.substring(1,rawStr.length()-1);
			availStorages.add(rawStr);
		}
		return availStorages;

	}

	@Override
	public boolean isDirectory(String storageSinkId, String directory) throws Exception {
		// /storages/{id}/files/{filePath}
		if(directory.equals("")) {
			directory = "/";
		}
		directory = directory.replace("//", "/");
		WebTarget target = getClient().target(getUnicoreBaseURI()).path("storages").path(storageSinkId).path("files").path(directory);
		Response resp = target.request(MediaType.APPLICATION_JSON).get();
		LOGGER.info("storages " + resp.getStatus() + ": " + resp.getStatusInfo());

		String res = resp.readEntity(String.class);
		javax.json.JsonReader reader = Json.createReader(new StringReader(res));
		javax.json.JsonObject jsonRespObj = reader.readObject();
		Boolean isDir = jsonRespObj.getBoolean("isDirectory");
		return isDir;
	}

	@Override
	public String getLocalPathOfRemoteFile(String storageSinkId, String directory) throws Exception {
		String lPath = directory;
		JsonObject strgSinkDescription = getInformationForSingleStorageSink(storageSinkId);
		lPath = strgSinkDescription.getString("mountPoint") + lPath;
		return lPath.replace("//", "/"); 
	}

}
