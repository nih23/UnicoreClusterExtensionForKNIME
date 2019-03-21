package org.tudresden.unicore;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import javax.json.JsonObject;

import org.knime.cluster.util.JobStatus;


/**
 * general interface for communication with unicore/unity systems
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public interface UnicoreConnector {
	
	static HashMap<String, JsonObject> m_storageSinkInformationCache = new HashMap<String, JsonObject>();

	
	public static String getCachedInformationForStorageSink(String strgSink) {
		if(m_storageSinkInformationCache.containsKey(strgSink))
			return m_storageSinkInformationCache.get(strgSink).getString("mountPoint");
		return null;
	}	
	/**
	 * retrieve list of availiable remote sites (queues)
	 * @return
	 * @throws Exception
	 */
	public LinkedList<String> getAvailiableSites() throws Exception;
	
	/**
	 * retrieve list of data sinks at remote site
	 * @return
	 * @throws Exception
	 */
	public LinkedList<String> getAvailiableStorageSites() throws Exception;
	
	/**
	 * retrieve detailed information for specified data sink
	 * @param storageSinkId
	 * @return
	 */
	public JsonObject getInformationForSingleStorageSink(String storageSinkId) throws Exception;
	
	/**
	 * upload file to specified remote data sink and path
	 * @param fileToUpload
	 * @param dataSinkId
	 * @param remotePath
	 * @return status
	 * @throws Exception
	 */
	public boolean uploadFile(File fileToUpload, String storageSinkId, String remotePath) throws Exception;
	
	/**
	 * retrieve binary file from specified remote data sink and path
	 * @param storageSinkId
	 * @param remotePath
	 * @param localSavePath
	 * @return status
	 * @throws Exception
	 */
	public byte[] downloadFile(String storageSinkId, String remotePath) throws Exception;
	
	/**
	 * submit job to queue on HPC system
	 * @param jobDescription job description in UCC format
	 * @return jobid
	 * @throws Exception
	 */
	public String submitJob(JsonObject jobDescription) throws Exception;
	
	
	/**
	 * start job in case of manual staging
	 * @param jobId
	 * @throws Exception
	 */
	public void startJob(String jobId, String desc) throws Exception;
	
	/**
	 * set name of queue on HPC system
	 * @param site
	 */
	public void setSitename(String site);
	
	/**
	 * get name of HPC queue
	 * @return
	 */
	public String getSite();
	
	/**
	 * returns batch system state
	 * @param jobId
	 * @return
	 */
	public JobStatus getStateOfJob(String jobId) throws Exception;
	
	public List<String> getListOfAllJobIDs() throws Exception;
	
	public List<String> listFiles(String storageSinkId, String directory) throws Exception;
	
	public boolean isDirectory(String storageSinkId, String directory) throws Exception;
	
	public void destroyJob(String jobId) throws Exception;
	
	public void openConnection() throws Exception;
	
	public void closeConnection() throws Exception;
	
	public String getLocalPathOfRemoteFile(String storageSinkId, String directory) throws Exception;
	
}
