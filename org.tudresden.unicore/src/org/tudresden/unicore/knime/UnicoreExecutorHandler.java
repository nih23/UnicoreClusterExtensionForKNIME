package org.tudresden.unicore.knime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.knime.cluster.StatusObserver;
import org.knime.cluster.api.ClusterHandler;
import org.knime.cluster.api.CommandExecutor;
import org.knime.cluster.util.JobStatus;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.tudresden.unicore.UnicoreConnector;
import org.tudresden.unicore.UnicoreRESTConnector;
import org.tudresden.unicore.knime.executor.settings.UnicoreExecutorConfiguration;
import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettings;

// Gateway: https://unicore.zih.tu-dresden.de:8080/
// Storage: default_storage
// Default sitename: TAURUS

public class UnicoreExecutorHandler implements ClusterHandler {

	private File m_fKnimeLog;
	private File m_fStdoutLog;
	private File m_fStderrLog;
	
	/* NEW API begin
	private final ClusterConfigurationRO m_configuration;
	private final ClusterHandler m_clusterHandler;
	   NEW API end */
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreExecutorHandler.class);

	private List<JsonObjectBuilder> m_stagingInformation;

	// private final ExecutionMonitor m_exec;

	//private final StatusObserver m_observer;

	// temporary directories for storing job related data
	private File m_jobDir_local = null;

	private UnicoreConnector m_unicore_connector;
	private String m_unicore_storageSinkId;
	private String m_unicore_storageMountPoint;
	private Boolean m_dataStagingEnabled;


	// if the job is submitted to unicore queue this id is set
	private String m_unicore_job_id = null;

	/** used to store reconnect info. */
	private static final String JOB_ID =
			"org.tudresden.unicore.knime.jobID";

	// other stuff
	private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
	private UnicoreExecutorConfiguration m_config;
	
	
	public UnicoreExecutorHandler(UnicoreExecutorConfiguration config) {
		m_config = config;
		m_dataStagingEnabled = false;
		m_unicore_connector = new UnicoreRESTConnector(
				config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Username), 
				config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Password), 
				config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY));
		m_unicore_connector.setSitename(config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE));
		m_unicore_job_id = null;
		m_unicore_storageSinkId = config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE);
		JsonObject strgSinkDescription;
		try {
			strgSinkDescription = m_unicore_connector.getInformationForSingleStorageSink(m_unicore_storageSinkId);
			m_unicore_storageMountPoint = strgSinkDescription.getString("mountPoint");
		} catch (Exception e) {
			LOGGER.error("An error occured while retrieving information about storage sink: " + e.getMessage(), e);
		}
	}
	
	@Override
	public void cancelJob(CommandExecutor arg0, String arg1) throws IOException {
		disconnect();
		
	}

	public void disconnect() {
		LOGGER.info("Disconnecting...");
		try {
			m_unicore_connector.destroyJob(m_unicore_job_id);
			m_unicore_job_id = null;
		} catch (Exception e) {
			LOGGER.error("Error while disconnecting from unicore: " + e.getMessage(), e);
		}
	}
	
	@Override
	public JobStatus checkJobStatus(CommandExecutor arg0, String arg1) throws IOException {
		try {
			JobStatus m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
			if(m_unicore_jobState == JobStatus.FINISHED) {
				downloadResultsOfFinishedJob();
			} else {
				retrieveAndWriteLogs();
			}
			return m_unicore_jobState;
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	public void downloadResultsOfFinishedJob() throws Exception {
//		
		// download zipped workflow			
		File localDirResultsFile = m_jobDir_local.getAbsoluteFile().getParentFile();
		String remoteNameOfResultsFile = m_jobDir_local.getName() + "_done.zip";
		LOGGER.info("Initiating download.");
		// check for staging
		byte[] data;
		
		if(m_dataStagingEnabled) {
			data = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", remoteNameOfResultsFile);
		} else {
			data = m_unicore_connector.downloadFile(m_unicore_storageSinkId, remoteNameOfResultsFile);
		}
		LOGGER.info("Downloading complete.");

		// unzip compressed remote executed workflow
		unzipZippedKnimeWorkflow(data,localDirResultsFile.getAbsolutePath());
	}
	
	/**
	 * Unzip results of remotely executed knime workflow
	 * @param bytecode of zip file
	 * @param output zip file output folder
	 */
	private void unzipZippedKnimeWorkflow(byte[] payload, String outputFolder){

		byte[] buffer = new byte[1024];

		try{
			//create output directory is not exists
			File folder = new File(outputFolder);
			if(!folder.exists()){
				folder.mkdir();
			}

			//get the zip file content
			ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(payload));
			//get the zipped file list entry
			ZipEntry ze; // = zis.getNextEntry();

			String rootDir = "";

			while((ze = zis.getNextEntry()) != null){
				long sz = ze.getSize();
				String fileName = ze.getName();
				if(fileName.startsWith(rootDir)) { // scratch/unicore/FILESPACE/storage/clusterjob_20170810033352_0_6704/
					fileName = fileName.replaceFirst(rootDir, "");
				}
				fileName = fileName.replace(m_unicore_storageMountPoint.substring(1,m_unicore_storageMountPoint.length()-1), "");

				//File newFile = new File(outputFolder + "_tmp" + File.separator + fileName);
				File newFile = new File(outputFolder + File.separator + fileName);
				if(newFile.exists() && newFile.isDirectory()) {
					continue;
				}

				LOGGER.info("decompressing "+ newFile.getAbsoluteFile());
				//create all non exists folders
				//else you will hit FileNotFoundException for compressed folder
				new File(newFile.getParent()).mkdirs();


				/*              if( (newFile.isFile() == false) ) {
           	  if(!newFile.exists()) {
           		  newFile.mkdir();
           	  }
           	  continue;
             }*/

				if(sz == 0) {
					if(!newFile.exists()) {
						newFile.mkdir();
					}
					continue;
				}

				try {

					FileOutputStream fos = new FileOutputStream(newFile);         

					int len;
					while ((len = zis.read(buffer)) > 0) {
						fos.write(buffer, 0, len);
					}
					fos.close();   
				} catch (Exception e) {
					System.out.println("");
				}

			}

			zis.closeEntry();
			zis.close();
			LOGGER.info("Decompression completed");

		}catch(IOException ex){
			LOGGER.warn(ex.getMessage(), ex);
		}
	} 
	
	@Override
	public void cleanup(CommandExecutor arg0, String arg1) throws IOException {
		disconnect();
		
//		// delete temporary folder
//		File localJobDir = getLocalJobDir();
//		FileUtils.deleteDirectory(localJobDir);
//
//		// delete zip file
//		File zippedWorkflow = new File(localJobDir.getAbsolutePath() + ".zip");
//		Boolean zipWorkflowDelState = zippedWorkflow.delete();
//
//		// delete remote script
//		Boolean unicoreScriptFileDelState = getLocalUnicoreScriptFile().delete();
//
//		m_unicore_connector.destroyJob(m_unicore_job_id);
//
//		// delete remote files
//		
//		return unicoreScriptFileDelState && zipWorkflowDelState;
		
	}

	@Override
	public String scheduleJob(CommandExecutor arg0, String scriptpath, String workflowpath) throws IOException {
		m_jobDir_local = new File(scriptpath).getParentFile();
		m_fKnimeLog = new File(m_jobDir_local + File.separator + "workspace/.metadata/knime/knime.log");
		m_fKnimeLog.getParentFile().mkdirs();
		m_fStdoutLog = new File(m_jobDir_local + File.separator + "standard-output.log");
		m_fStderrLog = new File(m_jobDir_local + File.separator + "standard-error.log");
		
		// load runtime script and modify paths
		fixKNIMEExecutionScript(new File(scriptpath));
		
		createUnicoreExecutionWorkflowScript();
		
		// zip data
		File dirWorkflowFiles = new File(m_jobDir_local.toString());
		String pZippedKnimeJobFolder = dirWorkflowFiles.getCanonicalPath() + ".zip";
		FileOutputStream fos = new FileOutputStream(pZippedKnimeJobFolder);
		ZipOutputStream zos = new ZipOutputStream(fos);
		LOGGER.info("Compressing workflow");
		try {
			addDirToZipArchive(zos, dirWorkflowFiles, null);
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		zos.flush();
		fos.flush();
		zos.close();
		fos.close();
		
		// zip payload folder
		File file = new File(pZippedKnimeJobFolder);
		
		/*
		 *  create job
		 */
		JsonObject unicoreJobDescription = createUnicoreJobDescription();
		try {
			m_unicore_connector.openConnection();
			m_unicore_connector.uploadFile(file, m_unicore_storageSinkId, getRemoteRootDir());
			m_unicore_connector.uploadFile(getLocalUnicoreScriptFile(), m_unicore_storageSinkId, getRemoteRootDir());
			m_unicore_job_id = m_unicore_connector.submitJob(unicoreJobDescription);
			PrintWriter statusWriter = new PrintWriter(m_fStdoutLog);
			statusWriter.println("JOB " + m_unicore_job_id + " has been started.");
			statusWriter.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
		
		/**
		 * this function retrieves log files resulting from remote execution and writes them into local files. 
		 * this is required for the knime logviewer to present the logs to the user
		 */
		private void retrieveAndWriteLogs() {
			try {
				byte[] dKnimeLog;
				byte[] dStdoutLog;
				byte[] dStderrLog;
				
				String pKnimeLog = "/workspace/.metadata/knime/knime.log";
				String pStdoutLog = "/standard-output.log";
				String pStderrLog = "/error-output.log";
				
				if(m_dataStagingEnabled) {
					dKnimeLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pKnimeLog);
					dStdoutLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pStdoutLog);
					dStderrLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pStderrLog);
				} else {
					dKnimeLog = m_unicore_connector.downloadFile(m_unicore_storageSinkId, m_jobDir_local.getName() + pKnimeLog);
					dStdoutLog = m_unicore_connector.downloadFile(m_unicore_storageSinkId, m_jobDir_local.getName() + pStdoutLog);
					dStderrLog = m_unicore_connector.downloadFile(m_unicore_storageSinkId, m_jobDir_local.getName() + pStderrLog);
				}
				
				
				BufferedWriter wKnimeLog = new BufferedWriter( new FileWriter( m_fKnimeLog ));
				wKnimeLog.write(new String(dKnimeLog));
				wKnimeLog.close();
				
				BufferedWriter wStdoutLog = new BufferedWriter( new FileWriter( m_fStdoutLog ));
				wStdoutLog.write(new String(dStdoutLog));
				wStdoutLog.close();
				
				BufferedWriter wStderrLog = new BufferedWriter( new FileWriter( m_fStderrLog ));
				wStderrLog.write(new String(dStderrLog));
				wStderrLog.close();
				
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
			}
		}
	
	private void fixKNIMEExecutionScript(File filename) {
	try {
		BufferedReader br = new BufferedReader(new FileReader(filename));
		StringBuilder sb = new StringBuilder();
		String line = br.readLine();

		while (line != null) {
			sb.append(line);
			sb.append(System.lineSeparator());
			line = br.readLine();
		}
		String wholeContent = sb.toString();
		wholeContent = wholeContent.replace("\r", "");
		wholeContent = wholeContent.replace(m_jobDir_local.getParent(), getRemoteRootDir());

		br.close();
		BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(filename));
		bufferedWriter.write(wholeContent);
		bufferedWriter.close();
	} catch (Exception e) {
		LOGGER.error(e.getMessage(), e);
		return;
	}
}
	
	private String getRemoteRootDir() {
		return m_unicore_storageMountPoint;
	}
	
	
	
	private File createUnicoreExecutionWorkflowScript() throws IOException {
		File script = getLocalUnicoreScriptFile();
		BufferedWriter writer = new BufferedWriter(new FileWriter(script));
		File fUnicoreWorkflowScript = null;
		try {
				if(m_dataStagingEnabled) {
					writeUnicoreWorkflowBashScriptStaging(writer);
				} else {
					writeUnicoreWorkflowBashScript(writer);
				}
			
		} finally {
			writer.close();
		}
		return fUnicoreWorkflowScript;
	}
	
	
	private void writeUnicoreWorkflowBashScript(BufferedWriter writer) throws IOException {
		String jobName = m_jobDir_local.getName();
		String remotePath = m_unicore_storageMountPoint + jobName;
	
		writer.write("#!/bin/bash");
		writer.newLine();
	
		// unzip payload
		writer.write("unzip " + remotePath + ".zip -d " + m_unicore_storageMountPoint );
		writer.write("\n");
	
		// invoke knime
		writer.write("sh " + remotePath + "/runWorkflow");
		writer.write("\n");
	
		// remove temporary stuff from payload folder
		writer.write("rm -rf " + remotePath + "/eclipse_config");
		writer.write("\n");
	
		// zip results
		writer.write("zip " + remotePath + "_done.zip -r " + remotePath);
		writer.write("\n");
	
		/*
		// cleanup
		writer.write("rm -rf " + remotePath + "/");
		writer.write("\n");
		writer.write("rm -rf " + remotePath + ".zip");
		writer.write("\n");
		*/
	}

private void writeUnicoreWorkflowBashScriptStaging(BufferedWriter writer) throws IOException {
	String jobName = m_jobDir_local.getName();
	String remotePath = getRemoteRootDir() + jobName;
	if(remotePath.startsWith("/")) {
		remotePath = remotePath.substring(1);
	}
	
	if(m_unicore_job_id == null) {
		LOGGER.error("Unicore Job ID not found. Something went wrong while preparing the remote execution script. Your job is probably going to fail.\n");
	}
	
	
	String jobid_uspace = m_unicore_job_id + "-uspace";
	String uspacepath = "";
	
	try {
		JsonObject storageSinkInfo = m_unicore_connector.getInformationForSingleStorageSink(jobid_uspace);
		uspacepath = storageSinkInfo.getString("mountPoint");		
	} catch (Exception e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
	
	writer.write("#!/bin/bash");
	writer.newLine();

	// change working directory
	writer.write("cd " + uspacepath);
	writer.write("\n");

	
	// unzip payload
	writer.write("unzip " + jobName + ".zip");
	writer.write("\n");
	
	writer.write("cd " + remotePath);
	writer.write("\n");

	// set staging dir
	writer.write("export UNICORE_STAGING_DIRECTORY=" + uspacepath);
	writer.write("\n");
	
	// invoke knime
	writer.write("sh runWorkflow");
	writer.write("\n");
	
	// process results
	writer.write("cd ..");
	writer.write("\n");

	// remove temporary stuff from payload folder
	writer.write("rm -rf " + remotePath + "/eclipse_config");
	writer.write("\n");

	// zip results
	writer.write("zip " + remotePath + "_done.zip -r " + remotePath);
	writer.write("\n");

	// cleanup
	/*writer.write("rm -rf " + remotePath + "/");
	writer.write("\n");
	writer.write("rm -rf " + remotePath + ".zip");
	writer.write("\n");*/
}
	
	
	/**
	 * Return the local filename of the script that is executed on the cluster. The file is located in the local job
	 * directory.
	 *
	 * @return the local filename of the script that is executed on the cluster
	 */
	protected File getLocalUnicoreScriptFile() {
		return new File(m_jobDir_local.getParent(), "Unicore_" + m_jobDir_local.getName() + ".sh");
	}

	private JsonObject createUnicoreJobDescription() {
		String jobName = m_jobDir_local.getName();
		String remotePath = m_unicore_storageMountPoint + jobName;
		String site = m_config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE);
		String memory = m_config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY);
		String runtime = m_config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME);
		String nodes = m_config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES);
		String cpusPerNode = m_config.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE);
	
		JsonObjectBuilder resourcesArrayBuilder = Json.createObjectBuilder();
		resourcesArrayBuilder.add("Memory", memory);
		resourcesArrayBuilder.add("Runtime", runtime);
		resourcesArrayBuilder.add("Nodes", nodes);
		resourcesArrayBuilder.add("CPUsPerNode", cpusPerNode);
	
		/*// Data Staging
		JsonArrayBuilder stagingArrayBuilder = Json.createArrayBuilder();
		for(JsonObjectBuilder stagingRedirectStdoutArrayBuilder : m_stagingInformation) {
			stagingArrayBuilder.add(stagingRedirectStdoutArrayBuilder);
		}*/
	
		JsonArrayBuilder exportsArrayBuilder = Json.createArrayBuilder();
		JsonObjectBuilder exportRedirectStdoutArrayBuilder = Json.createObjectBuilder();
		exportRedirectStdoutArrayBuilder.add("From", "stdout");
		exportRedirectStdoutArrayBuilder.add("To", remotePath + "/stdout");
		exportsArrayBuilder.add(exportRedirectStdoutArrayBuilder);
	
		JsonObjectBuilder builder = Json.createObjectBuilder();
		builder.add("Site", site);
		builder.add("ApplicationName", "knime");
		builder.add("Name", jobName);
		//builder.add("Executable", "sh " + getLocalUnicoreScriptFile().getName()); // TODO: changed 0605-2016
		
		builder.add("Executable", "sh " + m_unicore_storageMountPoint + getLocalUnicoreScriptFile().getName());
		
		builder.add("Resources", resourcesArrayBuilder);
	
		if(m_dataStagingEnabled) {
			// Data Staging
			//builder.add("Imports", stagingArrayBuilder);
		} else {
			// data is already on remote execution site -> no staging required
			builder.add("haveClientStageIn", false); 
		}
		builder.add("Exports", exportsArrayBuilder);
	
		return builder.build();
	}
	
	private void addDirToZipArchive(ZipOutputStream zos, File fileToZip, String parrentDirectoryName) throws Exception {
		if (fileToZip == null || !fileToZip.exists()) {
			return;
		}
	
		String zipEntryName = fileToZip.getName();
		if (parrentDirectoryName!=null && !parrentDirectoryName.isEmpty()) {
			zipEntryName = parrentDirectoryName + "/" + fileToZip.getName();
		}
	
		if (fileToZip.isDirectory()) {
			LOGGER.info("+" + zipEntryName);
			for (File file : fileToZip.listFiles()) {
				addDirToZipArchive(zos, file, zipEntryName);
			}
		} else {
			LOGGER.info("   " + zipEntryName);
			byte[] buffer = new byte[1024];
			FileInputStream fis = new FileInputStream(fileToZip);
			zos.putNextEntry(new ZipEntry(zipEntryName));
			int length;
			while ((length = fis.read(buffer)) > 0) {
				zos.write(buffer, 0, length);
			}
			zos.closeEntry();
			fis.close();
		}
	}
}
