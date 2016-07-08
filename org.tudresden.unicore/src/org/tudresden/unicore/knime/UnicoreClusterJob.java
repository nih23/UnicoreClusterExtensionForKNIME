package org.tudresden.unicore.knime;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.LinkedList;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import javax.json.Json;
import javax.json.JsonArrayBuilder;
import javax.json.JsonObject;
import javax.json.JsonObjectBuilder;

import org.apache.commons.io.FileUtils;
import org.knime.cluster.executor.AbstractClusterJob;
import org.knime.cluster.executor.AbstractClusterJobManager;
import org.knime.cluster.executor.AbstractClusterJobManager.JobStatus;
import org.knime.cluster.executor.JobPhaseSynchNoWait;
import org.knime.cluster.executor.settings.ClusterJobExecSettings;
import org.knime.cluster.executor.settings.ClusterJobExecSettings.InvokeScriptShell;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataType;
import org.knime.core.data.container.CloseableRowIterator;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeExecutionJobReconnectException;
import org.tudresden.unicore.UnicoreConnector;
import org.tudresden.unicore.UnicoreRESTConnector;
import org.tudresden.unicore.knime.filehandling.integration.UnicoreFileIntegrationNodeModel;
import org.tudresden.unicore.knime.filehandling.integration.UnicoreRemoteFileDataCell;


/**
 * control the life cycle of a HPC job that is being submitted by unicore 
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreClusterJob extends AbstractClusterJob {

	private File m_fKnimeLog;
	private File m_fStdoutLog;
	private File m_fStderrLog;
	
	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreClusterJob.class);

	private List<JsonObjectBuilder> m_stagingInformation;

	private final ExecutionMonitor m_exec;

	private final UnicoreNodeExecutionJobManager m_gje;

	private AbstractClusterJobManager.JobStatus m_unicore_jobState;

	// temporary directories for storing job related data
	private String m_jobDir_local = null;

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

	
	/**
	 * Creates a new node execution job.
	 *
	 * @param gje the Unicore job manager that created us
	 * @param nc the node to execute
	 * @param exec The execution context used to report progress for this job
	 * @param inData the data coming in on the node's ports
	 * @param preExecSync the job notifies this object after it finishes its
	 *            beforeExecute and waits on this object then. If null, the job
	 *            just continues without wait.
	 * @param postExecSync the object synchronizing all concurrent and dependent
	 *            grid jobs at the end of their mainExecute phase (before
	 *            entering afterExecution). If null, the job just continues
	 *            without wait.
	 * @param jobIdx index of job in the chunk, in case of a split job
	 * @param numOfJobs determines sub-progress
	 * @throws IOException 
	 */
	public UnicoreClusterJob(final UnicoreNodeExecutionJobManager gje, final NodeContainer nc,
			final ExecutionMonitor exec, final PortObject[] inData,
			final JobPhaseSynchNoWait preExecSync,
			final JobPhaseSynchNoWait postExecSync, final int jobIdx,
			final int numOfJobs) {
		
		super(nc, inData, preExecSync, postExecSync, gje.getSettings());

		m_dataStagingEnabled = false;
		m_gje = gje;
		m_exec = exec;
		m_unicore_connector = new UnicoreRESTConnector(m_gje.getSettings().getUsername(), m_gje.getSettings().getPassword(), m_gje.getSettings().getGateway());
		m_unicore_connector.setSitename(m_gje.getSettings().getDefaultSitename());
		m_unicore_jobState = AbstractClusterJobManager.JobStatus.ID_UNKNOWN;
		m_unicore_job_id = null;

		/*
		 * retrieve remote path of storage sink
		 */
		m_unicore_storageSinkId = m_gje.getSettings().getStorage();
		JsonObject strgSinkDescription;
		try {
			strgSinkDescription = m_unicore_connector.getInformationForSingleStorageSink(m_unicore_storageSinkId);
			m_unicore_storageMountPoint = strgSinkDescription.getString("mountPoint");
		} catch (Exception e) {
			LOGGER.error("An error occured while retrieving information about storage sink: " + e.getMessage(), e);
		}

		/*
		 * prepare information for data staging
		 */
		int splitPort = m_gje.getSettings().getSplitPortIdx() + 1;
		PortObject poSplitPort = inData[splitPort];
		if(!( poSplitPort instanceof BufferedDataTable))
			return;

		m_stagingInformation = new LinkedList<JsonObjectBuilder>();
	
		BufferedDataTable bdt = (BufferedDataTable)poSplitPort;
		int colIdx = bdt.getSpec().findColumnIndex(UnicoreFileIntegrationNodeModel.resolvedPathColumnName);
		if(colIdx == -1)
			return;

		DataType dt = bdt.getSpec().getColumnSpec(colIdx).getType();
		if(! dt.getCellClass().equals(UnicoreRemoteFileDataCell.class) ) 
			return;

		DataRow dr;	
		
		CloseableRowIterator cri = bdt.iterator();
		while (cri.hasNext() )
		{
			dr = cri.next();
			UnicoreRemoteFileDataCell dc = (UnicoreRemoteFileDataCell) dr.getCell(colIdx);
			if(dc.getValue().isIntegratedViaStaging())
			{
				JsonObjectBuilder stagingRedirectStdoutArrayBuilder = Json.createObjectBuilder();
				stagingRedirectStdoutArrayBuilder.add("From", dc.getValue().getURIForDataStaging());
				stagingRedirectStdoutArrayBuilder.add("To", dc.getStringValue().replace("file://", ""));
				stagingRedirectStdoutArrayBuilder.add("ReadOnly", "true");
				m_stagingInformation.add(stagingRedirectStdoutArrayBuilder);
			}
		}
		
		m_dataStagingEnabled = m_stagingInformation.size() > 0;
	}

	/**
	 * This constructor should only be used to reconnect to a running grid job.
	 * It initializes the variables in this instance only partially. The job is
	 * set to find an already submitted grid job, wait for it to finish and to
	 * pull its results back in.
	 *
	 * @param gje the job manager instantiating this
	 * @param nc node to reconnect
	 * @param exec The execution context used to report progress for this job
	 * @param reconnectSettings the info needed to reconnect to a running grid
	 *            job.
	 * @param postExecSync synch object to notify when the job returns from the
	 *            cluster
	 * @param jobIdx index of job in the chunk, in case of a split job
	 * @param numOfJobs not used.
	 * @throws InvalidSettingsException if provided settings are invalid
	 * @throws NodeExecutionJobReconnectException if reconnect fails.
	 * @throws IOException usually not
	 * @see #call()
	 */
	public UnicoreClusterJob(final UnicoreNodeExecutionJobManager gje, final NodeContainer nc,
			final ExecutionMonitor exec,
			final NodeSettingsRO reconnectSettings,
			final JobPhaseSynchNoWait postExecSync, final int jobIdx,
			final int numOfJobs) throws InvalidSettingsException, IOException,
	NodeExecutionJobReconnectException {

		super(nc, postExecSync, gje.getSettings(), reconnectSettings);

		m_gje = gje;
		m_exec = exec;
		m_unicore_connector = new UnicoreRESTConnector(m_gje.getSettings().getUsername(), m_gje.getSettings().getPassword(), m_gje.getSettings().getGateway());
		m_unicore_connector.setSitename(m_gje.getSettings().getDefaultSitename());

		/*
		 * retrieve remote path of storage sink
		 */
		m_unicore_storageSinkId = m_gje.getSettings().getStorage();
		JsonObject strgSinkDescription;
		try {
			strgSinkDescription = m_unicore_connector.getInformationForSingleStorageSink(m_unicore_storageSinkId);
			m_unicore_storageMountPoint = strgSinkDescription.getString("mountPoint");
		} catch (Exception e) {
			LOGGER.error("An error occured while retrieving information about storage sink: " + e.getMessage(), e);
		}

		// restore reconnect info
		m_unicore_job_id = reconnectSettings.getString(JOB_ID);
		// set unicore job state?
		//m_unicore_jobState = UnicoreJobState.unknown;

	}

	@Override
	protected String getRemoteControlPreScript() {
		return new StringBuilder(getRemoteControlDir()).append(KNIME_CONTROL_PRE_SCRIPT).toString();
	}

	/**
	 * Return the full absolute remote path to the script executed after the
	 * call to the batch executor.
	 *
	 * @return the full absolute remote path to the script executed after the
	 *         call to the batch executor
	 */
	@Override
	protected String getRemoteControlPostScript() {
		return new StringBuilder(getRemoteControlDir()).append(KNIME_CONTROL_POST_SCRIPT).toString();
	}

	/**
	 * Return the full absolute path to the sync file as seen from the remote
	 * cluster machines (this file is created remotely after KNIME finishes and
	 * is waited for locally by the local KNIME instance, to sync the remote and
	 * local file system).
	 *
	 * @return the absolute remote path to the sync file
	 */
	@Override
	protected String getRemoteSyncFile() {
		//// TODO: If the remote host has a different OS (for SSH) this fails!!
		//return new File(getRemoteJobDir(), JOB_SYNC_FILE).getAbsolutePath();
		return new StringBuilder(getRemoteControlDir()).append(JOB_SYNC_FILE).toString();
	}

	/**
	 * Return the full absolute subdir where pre/post execute scripts can be
	 * placed. This is the path as seen from the remote cluster machines.
	 *
	 * @return the full absolute remote subdir path where pre/post execute
	 *         scripts can be placed
	 */
	@Override
	protected String getRemoteControlDir() {
		if(m_gje.getSettings().getScriptShell().equals(InvokeScriptShell.batch)) {
			throw new IllegalStateException(
					"MS Batch scripts not supported by Unicore extension yet");
		}
		
		if(m_dataStagingEnabled) {
			return "";
		}
		
		
		
		String res = new StringBuilder(m_unicore_storageMountPoint).append(m_gje.getSettings().getRemoteRootDir()).append("/"+super.getLocalJobDir().getName()+"/").toString();

		if(!res.endsWith("/")) {
			res += "/";
		}    	
		return res; 
	}

	@Override
	protected File createNewLocalJobDirectory(final File parent) throws IOException {
		// create temp directory in shared folder, it will be solely used
		// by this job
		String dataField = DATE_FORMAT.format(new Date());
		File jobDir = createTempDir("knimeJob_" + dataField + "_", parent);
		m_jobDir_local = jobDir.getAbsolutePath();
		return new File(m_jobDir_local);
	}

	@Override
	protected String createNewRemoteJobDirectory(String userRemoteDir) throws IOException {
		String dataField = DATE_FORMAT.format(new Date());
		String jobDir = new StringBuilder("").append(userRemoteDir)  
				.append("/knimeJob_" + dataField + "/").toString();
		//TODO: IMPLEMENT REST CALL THAT ACTUALLY CREATES THE DIRECTORY
		return jobDir;
	}

	/**
	 * Return the full absolute path to the workflow directory. This is the path
	 * on the remove cluster machine. May not exist locally.
	 *
	 * @return the full absolute path to the workflow directory (remotely, may
	 *         not exist locally)
	 */
	@Override
	protected String getRemoteWorkflowDir() {
		return new StringBuilder(getRemoteControlDir()).append(WORKFLOW_DIR).toString();
	}

	/**
	 * Returns the full absolute path to the runtime dir used by the remote
	 * KNIME. The path can only be used remotely and may not exist locally.
	 *
	 * @return the full abstract path to the runtime dir - remotely, must not
	 *         exist locally.
	 */
	@Override
	protected String getRemoteRuntimeDir() {
		return new StringBuilder(getRemoteControlDir()).append(RUNTIME_DIR).toString();
	}

	/**
	 * @return the full path to the ini file created for the KNIME instance on
	 *         the cluster as seen from the remote cluster machine.
	 */
	@Override
	protected String getRemoteTempIniFile() {
		return new StringBuilder(getRemoteControlDir()).append(TEMP_INI_FILE).toString();
	}

	/**
	 * This returns the filename - as seen from the remote cluster machines - where preferences are stored in.
	 *
	 * @return the remote filename where preferences are stored in
	 */
	protected String getRemotePreferencesFile() {
		// TODO: If the remote host has a different OS (for SSH) this fails!!
		//return new File(getRemoteJobDir(), PREFERENCE_FILE).getAbsolutePath();
		return new StringBuilder(getRemoteControlDir()).append(PREFERENCE_FILE).toString();
	}

	/**
	 * Creates all command line arguments for the batch executor (including
	 * runtime workspace, workflow to execute, and preference file (if exported)
	 * as they should be used on the remote machine (all paths used are remote
	 * paths). It also includes the custom arguments returned by
	 * {@link ClusterJobExecSettings#getCustomKnimeArguments()}.
	 *
	 * @return list of command line arguments for the remote batch executor
	 * @throws IOException
	 */
	@Override
	public List<String> createExecArguments() throws IOException {
		ArrayList<String> result = new ArrayList<String>();
		result.add("-data");
		result.add(getRemoteRuntimeDir());
		result.add("-workflowDir=" + getRemoteWorkflowDir());
		if (m_gje.getSettings().exportPreferencesToCluster()) {
			result.add("-preferences=" + getRemotePreferencesFile());
		}
		result.add("-nosplash");
		result.add("--launcher.suppressErrors");
		result.add("-configuration");

		String pConfigArea = new StringBuilder(getRemoteControlDir()).append("eclipse_config").toString();
		result.add(pConfigArea);
		//result.add("-Dunicore.staging.directory=$UNICORE_STAGING_DIRECTORY");
		result.add("-application");
		result.add("org.knime.product.KNIME_BATCH_APPLICATION");
		if (!customSettingsUseIniFile()) {
			if (createIniFile()) {
				result.add("--launcher.ini");
				result.add(getRemoteTempIniFile());
			}
		}
		if (m_gje.getSettings().getCustomKnimeArguments() != null) {
			result.addAll(m_gje.getSettings().getCustomKnimeArguments());
		}
		return result;
	}

	/**
	 * @return true if creation of the ini file was successful, false otherwise
	 */
	@Override
	protected boolean createIniFile() {
		/* this uses the ini file from the currently running KNIME instance as
		 * template for the cluster KNIME ini file. Ideally the file from the
		 * remote installation should be used. But it is not accessible. (The
		 * specified path to the KNIME exe could denote a script, not the
		 * executable.)
		 */
		File iniTemplate = getCurrentIniFile();
		if (iniTemplate == null) {
			return false;
		}
		File newIniFile = createClusterIniFile(iniTemplate);
		if (newIniFile == null) {
			return false;
		}
		return newIniFile.exists();
	}

	private File createClusterIniFile(final File origIniFile) {
		String os = System.getProperty("os.name").toLowerCase();

		// copy the original file into the new ini file
		boolean containsVMargs = false;
		File clusterIniFile = getLocalTempIniFile();
		BufferedWriter iniWriter = null;
		BufferedReader iniReader = null;
		try {
			try {
				iniWriter = new BufferedWriter(new FileWriter(clusterIniFile));
			} catch (IOException e) {
				LOGGER.warn("Unable to create cluster KNIME ini file: " + e.getMessage(), e);
				return null;
			}
			try {
				iniReader = new BufferedReader(new FileReader(origIniFile));
			} catch (FileNotFoundException e) {
				LOGGER.warn("Unable to create cluster KNIME ini file: " + e.getMessage(), e);
				return null;
			}
			try {
				String line = null;
				while ((line = iniReader.readLine()) != null) {               	
					if (line.trim().toLowerCase().startsWith("-vmargs")) {
						containsVMargs = true;
					}
					String lineLower = line.toLowerCase();

					// thats a very specific workaround (hack) for multi-system use of eclipse unicore cluster extension.
					// the following lines skip some libraries of eclipse/knime that are system-dependent and would prevent knime execution on linux/unix
					// systems, if the jobs are created on a windows system
					if(os.contains("windows")) {
						if (lineLower.contains("-startup") || lineLower.contains(".equinox.") || lineLower.contains("--launcher.library") ) {
							continue;
						}
					}
					iniWriter.write(line + "\n");
				}
				iniReader.close();
				if (!containsVMargs) {
					iniWriter.write("-vmargs\n");
				}
				iniWriter.write("-Djava.awt.headless=true\n");
				iniWriter.close();
				return clusterIniFile;
			} catch (IOException e) {
				LOGGER.warn("Unable to create cluster KNIME ini file: " + e.getMessage(), e);
				return null;
			}
		} finally {
			if (iniWriter != null) {
				try {
					iniWriter.close();
				} catch (IOException e) {
					// ignore
				}
			}
			if (iniReader != null) {
				try {
					iniReader.close();
				} catch (IOException e) {
					// ignore
				}
			}
		}
	}

	@Override
	public ExecutionMonitor getExecutionMonitor() {
		return m_exec;
	}

	@Override
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
	protected void cancelJob() {
		try {
			LOGGER.info("Aborting job " + m_unicore_job_id);
			m_unicore_connector.destroyJob(m_unicore_job_id);
			m_unicore_job_id = "";
		} catch (Exception e) {
			LOGGER.error("Error while canceling job " + m_unicore_job_id + ": " + e.getMessage(), e);
		}
	}

	@Override
	public String getJobID() {
		return m_unicore_job_id;
	}

	@Override
	public void reconnectToRemoteJob() {
		// we only need to update our state, everything else should be fine
		try {
			m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
		} catch (Exception e) {
			LOGGER.error("An error occured while reconnecting to remote job " + m_unicore_job_id, e);
		}
	}

	private void fixLinebreaks(File filename) {
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
			wholeContent = wholeContent.replace("bash -l", "bash");

			br.close();
			BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(super.getLocalScriptFile()));
			bufferedWriter.write(wholeContent);
			bufferedWriter.close();
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
			return;
		}
	}
	
	protected void submitClusterJobWithStaging() throws Exception {
		m_unicore_connector.openConnection();
		
		/*
		 *  create job
		 */
		JsonObject unicoreJobDescription = createUnicoreJobDescription();
		//FileUtils.writeStringToFile(new File("c:\\Users\\nico\\jobDesc.txt"), unicoreJobDescription.toString());
		
		/*
		 *  submit job (unzip stuff + exec. workflow) + update job properties
		 */
		m_unicore_job_id = m_unicore_connector.submitJob(unicoreJobDescription);
		String jobid_uspace = m_unicore_job_id + "-uspace";
		/*
		 * now we update all paths with its respective correlates
		 */

		// replace by remote path
		
		/*
		 * prepare workflow and all data
		 */	
		createUnicoreExecutionWorkflowScript();

		// zip knime folder with workflow
		File dirWorkflowFiles = super.getLocalJobDir();	
		String pZippedKnimeJobFolder = dirWorkflowFiles.getCanonicalPath() + ".zip";
		FileOutputStream fos = new FileOutputStream(pZippedKnimeJobFolder);
		ZipOutputStream zos = new ZipOutputStream(fos);
		LOGGER.info("Compressing workflow");
		addDirToZipArchive(zos, dirWorkflowFiles, null);
		zos.flush();
		fos.flush();
		zos.close();
		fos.close();

		// zip payload folder
		File file = new File(pZippedKnimeJobFolder);

		// upload knime workflow
		m_unicore_connector.uploadFile(file, jobid_uspace, m_gje.getSettings().getRemoteRootDir());
		m_unicore_connector.uploadFile(getLocalUnicoreScriptFile(), m_unicore_storageSinkId, m_gje.getSettings().getRemoteRootDir());

	
		/*
		 *  start data staging + our job
		 */		
		m_unicore_connector.startJob(m_unicore_job_id, unicoreJobDescription.toString());

	}
	
	protected void submitClusterJobWithoutStaging() throws Exception {
		createUnicoreExecutionWorkflowScript();

		/*
		 *  upload files
		 */
		// zip knime folder with workflow
		File dirWorkflowFiles = super.getLocalJobDir();	
		String pZippedKnimeJobFolder = dirWorkflowFiles.getCanonicalPath() + ".zip";
		FileOutputStream fos = new FileOutputStream(pZippedKnimeJobFolder);
		ZipOutputStream zos = new ZipOutputStream(fos);
		LOGGER.info("Compressing workflow");
		addDirToZipArchive(zos, dirWorkflowFiles, null);
		zos.flush();
		fos.flush();
		zos.close();
		fos.close();

		// zip payload folder
		File file = new File(pZippedKnimeJobFolder);
		m_unicore_connector.openConnection();

		/*
		 *  create job
		 */
		JsonObject unicoreJobDescription = createUnicoreJobDescription();
		
		//FileUtils.writeStringToFile(new File("c:\\Users\\nico\\jobDesc.txt"), unicoreJobDescription.toString());
		
		m_unicore_connector.uploadFile(file, m_unicore_storageSinkId, m_gje.getSettings().getRemoteRootDir());
		m_unicore_connector.uploadFile(getLocalUnicoreScriptFile(), m_unicore_storageSinkId, m_gje.getSettings().getRemoteRootDir());
		
		/*
		 *  submit job (unzip stuff + exec. workflow) + update job properties
		 */
		m_unicore_job_id = m_unicore_connector.submitJob(unicoreJobDescription);
	}

	@Override
	protected void submitClusterJob() throws Exception {
		// if the script file is created on windows systems and then executed remotely on linux clusters, its execution will fail due to \r\n line breaks
		// this bug results from the script creation method of @AbstractClusterJob		
		fixLinebreaks(super.getLocalScriptFile());		
		
		try
		{
		if(m_dataStagingEnabled == true) {
			submitClusterJobWithStaging();
		} else {
			submitClusterJobWithoutStaging();
		}
		} catch (Exception e) {
			LOGGER.warn(e.getMessage());
		}
		
		if(m_unicore_job_id == null) {
			LOGGER.error("Aborting execution");
			m_unicore_connector.closeConnection();
			m_unicore_jobState = JobStatus.ERROR;
			return;
		}
		
		m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
		m_unicore_connector.closeConnection();
		LOGGER.info("Job " + m_unicore_job_id + " sucessfully submitted.");
		m_fKnimeLog = new File(getLocalJobDir() + m_unicore_job_id + "_knimelog.txt");
		m_fStdoutLog = new File(getLocalJobDir() + m_unicore_job_id + "_stdout.txt");
		m_fStderrLog = new File(getLocalJobDir() + m_unicore_job_id + "_stderr.txt");
	}

	private JsonObject createUnicoreJobDescription() {
		String jobName = super.getLocalJobDir().getName();
		String remotePath = m_unicore_storageMountPoint + m_gje.getSettings().getRemoteRootDir() + jobName;
		String site = m_gje.getSettings().getDefaultSitename();
		String memory = m_gje.getSettings().getJobMemory();
		String runtime = m_gje.getSettings().getJobRuntime();
		String nodes = m_gje.getSettings().getJobNodes();
		String cpusPerNode = m_gje.getSettings().getJobCPUsPerNode();

		JsonObjectBuilder resourcesArrayBuilder = Json.createObjectBuilder();
		resourcesArrayBuilder.add("Memory", memory);
		resourcesArrayBuilder.add("Runtime", runtime);
		resourcesArrayBuilder.add("Nodes", nodes);
		resourcesArrayBuilder.add("CPUsPerNode", cpusPerNode);

		// Data Staging
		JsonArrayBuilder stagingArrayBuilder = Json.createArrayBuilder();
		for(JsonObjectBuilder stagingRedirectStdoutArrayBuilder : m_stagingInformation) {
			stagingArrayBuilder.add(stagingRedirectStdoutArrayBuilder);
		}

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
		
		builder.add("Executable", "sh " + m_unicore_storageMountPoint + m_gje.getSettings().getRemoteRootDir() + getLocalUnicoreScriptFile().getName());
		
		builder.add("Resources", resourcesArrayBuilder);

		if(m_dataStagingEnabled) {
			// Data Staging
			builder.add("Imports", stagingArrayBuilder);
		} else {
			// data is already on remote execution site -> no staging required
			builder.add("haveClientStageIn", false); 
		}
		builder.add("Exports", exportsArrayBuilder);

		return builder.build();
	}

	@Override
	public boolean cleanup(boolean success) {
		try {
			if(m_unicore_job_id == null) {
				return true;
			}
			// delete temporary folder
			File localJobDir = getLocalJobDir();
			FileUtils.deleteDirectory(localJobDir);

			// delete zip file
			File zippedWorkflow = new File(localJobDir.getAbsolutePath() + ".zip");
			Boolean zipWorkflowDelState = zippedWorkflow.delete();

			// delete remote script
			Boolean unicoreScriptFileDelState = getLocalUnicoreScriptFile().delete();

			m_unicore_connector.destroyJob(m_unicore_job_id);

			// delete remote files
			
			return unicoreScriptFileDelState && zipWorkflowDelState;
		} catch (Exception e) {
			LOGGER.error("Couldnt cleanup job: " + e.getLocalizedMessage(), e);
			return false;
		}

	}

	@Override
	public JobStatus waitForJobToFinish() {
		try {
			m_gje.jobStartsExecute(this, getChunkJobIdx(), m_fKnimeLog, m_fStdoutLog, m_fStderrLog);
			getExecutionMonitor().setMessage("Waiting for job " + m_unicore_job_id + " to finish");
			m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
			long t1 = System.currentTimeMillis();
			while( (m_unicore_jobState == JobStatus.RUNNING) || (m_unicore_jobState == JobStatus.SIGNALED) ) {
				Thread.sleep(1000); // busy waiting ftw
				m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
				retrieveAndWriteLogs();
				System.out.print(".");
			}
			System.out.println("");
			long t2 = System.currentTimeMillis();
			LOGGER.error("Runtime of remote job " + (t2 - t1) + " ms");
			LOGGER.info(m_unicore_job_id + " finished with " + m_unicore_jobState.toString());
			retrieveAndWriteLogs();
			if(m_unicore_jobState == JobStatus.DONE) {
				// download zipped workflow			
				File localDirResultsFile = super.getLocalJobDir().getAbsoluteFile().getParentFile();
				String remoteNameOfResultsFile = super.getLocalJobDir().getName() + "_done.zip";
				LOGGER.info("Initiating download.");
				// check for staging
				byte[] data;
				
				if(m_dataStagingEnabled) {
					data = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", remoteNameOfResultsFile);
				} else {
					data = m_unicore_connector.downloadFile(m_unicore_storageSinkId, m_gje.getSettings().getRemoteRootDir() + remoteNameOfResultsFile);
				}
				LOGGER.info("Downloading complete.");

				// unzip compressed remote executed workflow
				unzipZippedKnimeWorkflow(data,localDirResultsFile.getAbsolutePath());
			} else {
				LOGGER.error(m_unicore_job_id + " finished in unexpected state " + m_unicore_jobState.toString());
			}
			m_gje.jobFinishes(this, getChunkJobIdx(), m_fKnimeLog, m_fStdoutLog, m_fStderrLog);
			// normally we would just return m_unicore_jobState, but for debugging purposes we currently use the following code
			switch(m_unicore_jobState) {
			case DONE: // download files
				return JobStatus.DONE;
			case RUNNING:
				throw new IllegalStateException("We shouldn't be in execution state after job execution has completed..");
			case ERROR:
				return JobStatus.ERROR;
			case FAILED:
				return JobStatus.FAILED;
			case SIGNALED:
				throw new IllegalStateException("We shouldn't be in submitted state after the job execution has completed..");
			case ID_UNKNOWN:
				m_unicore_jobState = m_unicore_connector.getStateOfJob(m_unicore_job_id);
				return JobStatus.ID_UNKNOWN;
			case DISCONNECTED:
				throw new IllegalStateException("We got disconnected from remote unicore site.");
			case ABORTED:
				throw new IllegalStateException("We got disconnected from remote unicore site.");
			}
		} catch (Exception e) {
			LOGGER.error("Error while waiting for job to finish: " + e.getLocalizedMessage(), e);
		}


		return null;
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
			//ZipInputStream zis = new ZipInputStream(new FileInputStream(zipFile));
			//ZipInputStream zis = new ZipInputStream(new ByteInputStream(payload, payload.length));
			ZipInputStream zis = new ZipInputStream(new ByteArrayInputStream(payload));
			//get the zipped file list entry
			ZipEntry ze; // = zis.getNextEntry();

			String rootDir = m_gje.getSettings().getRemoteRootDir();

			while((ze = zis.getNextEntry()) != null){
				long sz = ze.getSize();
				String fileName = ze.getName();
				if(fileName.startsWith(rootDir)) {
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


	private File createUnicoreExecutionWorkflowScript() throws IOException {
		File script = getLocalUnicoreScriptFile();
		BufferedWriter writer = new BufferedWriter(new FileWriter(script));
		File fUnicoreWorkflowScript = null;
		try {
			switch (m_gje.getSettings().getScriptShell()) {
			case bash:
				if(m_dataStagingEnabled) {
					writeUnicoreWorkflowBashScriptStaging(writer);
				} else {
					writeUnicoreWorkflowBashScript(writer);
				}
				break;
			case tcsh:
				throw new IllegalStateException("TCSH scripts not supported yet");
			case batch:
				throw new IllegalStateException("MS Batch scripts not supported yet");
			}
		} finally {
			writer.close();
		}

		return fUnicoreWorkflowScript;
	}

	private void writeUnicoreWorkflowBashScript(BufferedWriter writer) throws IOException {
		String jobName = super.getLocalJobDir().getName();
		String remotePath = m_unicore_storageMountPoint + m_gje.getSettings().getRemoteRootDir() + jobName;

		writer.write("#!/bin/bash");
		writer.newLine();

		// unzip payload
		writer.write("unzip " + remotePath + " -d " + m_unicore_storageMountPoint + m_gje.getSettings().getRemoteRootDir());
		writer.write("\n");

		// invoke knime
		writer.write("sh " + remotePath + "/InvokeKNIME.sh");
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
		String jobName = super.getLocalJobDir().getName();
		String remotePath = m_gje.getSettings().getRemoteRootDir() + jobName;
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
		writer.write("sh InvokeKNIME.sh");
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
		return new File(getLocalJobDir().getParent(), "Unicore_" + super.getLocalJobDir().getName() + ".sh");
	}


	private void retrieveAndWriteLogs() {
		try {
			byte[] dKnimeLog;
			byte[] dStdoutLog;
			byte[] dStderrLog;
			
			String pKnimeLog = "/runtimeWS/.metadata/knime/knime.log";
			String pStdoutLog = "/stdout";
			String pStderrLog = "/stderr";
			
			if(m_dataStagingEnabled) {
				dKnimeLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pKnimeLog);
			} else {
				dKnimeLog = m_unicore_connector.downloadFile(m_unicore_storageSinkId, m_gje.getSettings().getRemoteRootDir() + super.getLocalJobDir().getName() + pKnimeLog);
			}
			dStdoutLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pStdoutLog);
			dStderrLog = m_unicore_connector.downloadFile(m_unicore_job_id + "-uspace", pStderrLog);
			
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
			
		}
	}

}
