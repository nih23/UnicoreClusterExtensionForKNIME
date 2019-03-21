//package org.tudresden.unicore.knime;
//
//import java.io.BufferedInputStream;
//import java.io.BufferedOutputStream;
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.net.URL;
//import java.util.HashMap;
//import java.util.Map;
//import java.util.concurrent.CopyOnWriteArraySet;
//import java.util.concurrent.atomic.AtomicBoolean;
//
//import org.knime.cluster.executor.AbstractChunkedClusterJob;
//import org.knime.cluster.executor.AbstractClusterJob;
//import org.knime.cluster.executor.AbstractClusterJobManager;
//import org.knime.cluster.executor.JobPhaseSynchNoWait;
//import org.knime.cluster.view.BlankModel;
//import org.knime.cluster.view.JobManagerView;
//import org.knime.cluster.view.LogProvider;
//import org.knime.core.data.DataTableSpec;
//import org.knime.core.internal.ReferencedFile;
//import org.knime.core.node.ExecutionMonitor;
//import org.knime.core.node.InvalidSettingsException;
//import org.knime.core.node.NodeLogger;
//import org.knime.core.node.NodeSettings;
//import org.knime.core.node.NodeSettingsRO;
//import org.knime.core.node.NodeSettingsWO;
//import org.knime.core.node.port.PortObject;
//import org.knime.core.node.port.PortObjectSpec;
//import org.knime.core.node.workflow.NodeContainer;
//import org.knime.core.node.workflow.NodeContainer.NodeContainerSettings.SplitType;
//import org.knime.core.node.workflow.NodeExecutionJob;
//import org.knime.core.node.workflow.NodeExecutionJobManagerPanel;
//import org.knime.core.node.workflow.NodeExecutionJobReconnectException;
//import org.knime.core.util.FileUtil;
//import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettings;
//import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettingsPanel;
//import org.tudresden.unicore.knime.executor.settings.UnicorePreferenceInitializer;
//import org.tudresden.unicore.knime.filehandling.connector.UnicoreConnectionInformationPortObjectSpec;
//
///**
// * Handler for remote job lifecycle
// * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
// */
//public class UnicoreNodeExecutionJobManager extends AbstractClusterJobManager implements LogProvider {
//
//    private static final NodeLogger LOGGER =
//            NodeLogger.getLogger(UnicoreNodeExecutionJobManager.class);
//
//    // lists of all views created.
//    private final CopyOnWriteArraySet<JobManagerView> m_views =
//            new CopyOnWriteArraySet<JobManagerView>();
//    private UnicoreJobManagerSettings m_settings;
//    
//      // output
//    private boolean m_showMultipleFiles = false;
//    private final AtomicBoolean m_hasViewContentToSave = new AtomicBoolean(false);
//    private final AtomicBoolean m_contentFromPrevRun = new AtomicBoolean(false);
//    private final Map<Integer, File> m_logFile = new HashMap<Integer, File>();
//    private final Map<Integer, File> m_outFile = new HashMap<Integer, File>();
//    private final Map<Integer, File> m_errFile = new HashMap<Integer, File>();
//    
//    /**
//     * Don't instantiate this job manager explicitly. The core instantiates all
//     * extension of the corresponding extension point and maintains a pool.
//     */
//    public UnicoreNodeExecutionJobManager() {
//        m_settings = new UnicoreJobManagerSettings();
//    }
//    
//    @Override
//    public void load(final NodeSettingsRO settings)
//            throws InvalidSettingsException {
//        UnicoreJobManagerSettings s = new UnicoreJobManagerSettings(settings);
//        String errMsg = s.getStatusMsg();
//        if (errMsg != null) {
//            throw new InvalidSettingsException(errMsg);
//        }
//        m_settings = s;
//    }
//    
//    /**
//     * @return the settings
//     */
//    @Override
//    public UnicoreJobManagerSettings getSettings() {
//        return m_settings;
//    }
//	
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public String getID() {
//        return UnicoreNodeExecutionJobManagerFactory.ID;
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public NodeExecutionJobManagerPanel getSettingsPanelComponent(
//            final SplitType nodeSplitType) {
//        return new UnicoreJobManagerSettingsPanel(nodeSplitType);
//    }
//
//	@Override
//	public URL getIcon() {
//		return getClass().getResource("/unicoreIcon16x16.png");
//	}
//	
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void save(final NodeSettingsWO settings) {
//        m_settings.save(settings);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    protected AbstractClusterJob createNewClusterJob(final NodeContainer nc,
//            final ExecutionMonitor exec, final PortObject[] inData,
//            final JobPhaseSynchNoWait preExecSync, final JobPhaseSynchNoWait postExecSync,
//            final int jobIdx, final int numOfJobs) {
//        
//        return new UnicoreClusterJob(this, nc, exec, inData, preExecSync, postExecSync, jobIdx, numOfJobs);
//    }
//
//	@Override
//	protected AbstractChunkedClusterJob createNewChunkedClusterJob(NodeContainer nc, PortObject[] inPorts) {
//		return new ChunkedUnicoreJob(this, nc, inPorts, m_settings);
//	}
//
//	@Override
//	protected AbstractChunkedClusterJob createReconnectingChunkedClusterJob(NodeSettingsRO reconnectInfo,
//			PortObject[] inData, NodeContainer nc) throws InvalidSettingsException, NodeExecutionJobReconnectException {
//		return new ChunkedUnicoreJob(this, nc, inData, m_settings); // maybe trigger update of job state / use separate constructor
//	}
//	
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public synchronized NodeExecutionJob submitJob(final NodeContainer nc,
//            final PortObject[] data) {
//
//        // the node is about to execute: clear old view content
//        releaseViewContent();
//        if (m_settings.usePreferences()) {
//            m_settings = updateSettingsFromPrefPage(m_settings);
//        }
//
//        return super.submitJob(nc, data);
//    }
//    
//    /**
//     * Changes the values in the passed settings object. All values defined in
//     * the preference page are overridden with the current values in the
//     * preference page. All other settings remain unchanged. The passed object
//     * is returned.
//     *
//     * @param s the settings object to update from the preference page
//     * @return the object passed in, with the current values from the preference
//     *         page
//     */
//    static UnicoreJobManagerSettings updateSettingsFromPrefPage(final UnicoreJobManagerSettings s) {
//        UnicorePreferenceInitializer.getSettingsFromPreferences(s);
//        return s;
//    }
//    
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public PortObjectSpec[] configure(final PortObjectSpec[] inSpecs,
//            final PortObjectSpec[] nodeModelOutSpecs)
//            throws InvalidSettingsException {
//
//        if (m_settings.splitExecution() && nodeModelOutSpecs != null) {
//
//            // to each table add a column to mark failing split jobs
//            PortObjectSpec[] result =
//                    new DataTableSpec[nodeModelOutSpecs.length];
//            for (int i = 0; i < result.length; i++) {
//                PortObjectSpec s = nodeModelOutSpecs[i];
//                if (s == null) { // no table spec available
//                    continue;
//                }
//                if (!(s instanceof DataTableSpec) && !(s instanceof UnicoreConnectionInformationPortObjectSpec)) {
//                    // splitting can't be done on nodes with model output ports
//                    throw new InvalidSettingsException(
//                            "Splitting of the input data table "
//                                    + "can only be done if the node has no "
//                                    + "model output ports. "
//                                    + "Please change node settings.");
//                }
//                if (i == 0) {
//                    DataTableSpec nodeSpec =
//                            (DataTableSpec)nodeModelOutSpecs[i];
//                    result[i] = addSuccessFlagColumnToSpec(nodeSpec);
//                } else {
//                    result[i] = nodeModelOutSpecs[i];
//                }
//            }
//            return result;
//        } else {
//            return nodeModelOutSpecs;
//        }
//    }
//
//	@Override
//	protected AbstractClusterJob createReconnectingClusterJob(NodeContainer nc, ExecutionMonitor exec,
//			JobPhaseSynchNoWait postExecSync, NodeSettingsRO reconnectInfo, int jobIdx, int numOfJobs)
//					throws InvalidSettingsException, NodeExecutionJobReconnectException {
//		return null;
//	}
//	
//    /*
//     * -------------- view handling -----------------------------------
//     */
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean hasView() {
//        return true;
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public String getViewName(final NodeContainer nc) {
//        return "Unicore Cluster Computing Engine";
//    }
//
//    /** Mutex to synchronize access on one of the log file maps. */
//    private final Object m_fileMapMutex = new Object();
//
//    // names for the copied log files
//    private static final String STDOUT_FILENAME = "Stdout.txt";
//
//    private static final String STDERR_FILENAME = "Stderr.txt";
//
//    private static final String KNIMELOG_FILENAME = "KNIME_log.txt";
//
//    private static final String VIEWSETTINGS_FILENAME = "ViewSettings.xml";
//
//    private static final String CFGKEY_OLDCONTENT = "oldContent";
//
//    private static final String CFGKEY_MULTIPLEFILES = "multipleFiles";
//
//    // if not null, this is the place where saved log files are located
//    private ReferencedFile m_saveDirectory = null;
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public synchronized JobManagerView getView(final NodeContainer nc) {
//
//        // the view includes itself in the list of views
//        // (by registering with this)
//        JobManagerView result =
//                new JobManagerView(this, nc, new BlankModel());
//        result.configureView(m_showMultipleFiles);
//        // Show the files we may already have
//        if (m_logFile.isEmpty()) {
//            // see if we have files loaded in
//            if (m_saveDirectory != null) {
//                transferSavedFilesInMap(m_saveDirectory, STDOUT_FILENAME,
//                        m_outFile);
//                transferSavedFilesInMap(m_saveDirectory, STDERR_FILENAME,
//                        m_errFile);
//                transferSavedFilesInMap(m_saveDirectory, KNIMELOG_FILENAME,
//                        m_logFile);
//            }
//        }
//        result.showFiles(m_logFile, m_outFile, m_errFile);
//        if (m_contentFromPrevRun.get()) {
//            result.showWarning("Displays logs from previous run");
//        }
//
//        return result;
//    }
//
//    /**
//     * All views must register here in order to get notified about new settings
//     * and new files to show.
//     *
//     * @param v the view to register.
//     */
//    @Override
//    public void registerView(final JobManagerView v) {
//        m_views.add(v);
//    }
//
//    /**
//     * Call when a view closes. It doesn't receive file location notifications
//     * anymore.
//     *
//     * @param v the view to deregister
//     */
//    @Override
//    public void unregisterView(final JobManagerView v) {
//        m_views.remove(v);
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean parallelExecution() {
//        return getSettings().splitExecution();
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void resetAllViews() {
//        // we don't clear the view content but preserve it till the next execute
//        m_contentFromPrevRun.set(true);
//        for (JobManagerView view : m_views) {
//            view.showWarning("Displays logs from previous run");
//        }
//
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public void closeAllViews() {
//        for (JobManagerView view : m_views) {
//            view.closeView();
//        }
//    }
//
//    /**
//     * This must be called before the node is executed to clear any old content
//     * from the views.
//     */
//    private void releaseViewContent() {
//        m_showMultipleFiles = m_settings.splitExecution();
//        m_saveDirectory = null;
//        m_hasViewContentToSave.set(false);
//        m_contentFromPrevRun.set(false);
//        m_logFile.clear();
//        m_outFile.clear();
//        m_errFile.clear();
//
//        for (JobManagerView view : m_views) {
//            view.reset();
//            view.configureView(m_showMultipleFiles);
//        }
//
//    }
//
//    /**
//     * Called by the NodeExecutionJob to notify all views about the log file
//     * location. Also called when reconnecting.
//     *
//     * @param job the job that is about to start execution
//     * @param jobIdx if this is a split job, the index of the split - or zero
//     * @param logFile the knime log of this job
//     * @param outFile the standard output file of the job
//     * @param errFile the standard error file
//     */
//    void jobStartsExecute(final UnicoreClusterJob job, final int jobIdx,
//            final File logFile, final File outFile, final File errFile) {
//
//        synchronized (m_fileMapMutex) {
//            m_logFile.put(jobIdx, logFile);
//            m_outFile.put(jobIdx, outFile);
//            m_errFile.put(jobIdx, errFile);
//        }
//
//        for (JobManagerView view : m_views) {
//            view.showFiles(jobIdx, logFile, outFile, errFile);
//        }
//
//    }
//
//    /**
//     * Called by the NodeExecutionJob before it ends. This method copies all
//     * relevant log files into a temporary directory (before they (possibly) get
//     * deleted).
//     *
//     * @param job the job that is about to end.
//     * @param jobIdx in case it is a split job - otherwise zero
//     * @param logFile the knime log file of this job
//     * @param outFile the standard output file of the job
//     * @param errFile the standard error file
//     */
//    void jobFinishes(final UnicoreClusterJob job, final int jobIdx,
//            final File logFile, final File outFile, final File errFile) {
//
//        m_hasViewContentToSave.set(true);
//
//        // copy the files before they get deleted
//        File dir;
//        try {
//            dir =
//                    FileUtil.createTempDir("SGE_TempLogFiles_ID"
//                            + job.getJobID() + "_split" + jobIdx);
//        } catch (IOException e) {
//            // then don't save them - maybe the original files stay...
//            LOGGER.info("Unable to preserve log files (couldn't create temp "
//                    + "directory - grid job id " + job.getJobID() + ").");
//            return;
//        }
//        File newLogFile = new File(dir, KNIMELOG_FILENAME);
//        File newOutFile = new File(dir, STDOUT_FILENAME);
//        File newErrFile = new File(dir, STDERR_FILENAME);
//        try {
//            if (logFile != null && logFile.exists()) {
//                FileUtil.copy(logFile, newLogFile);
//            } else {
//                LOGGER.info("KNIME log file doesn't exist (grid job id "
//                        + job.getJobID() + ")");
//                newLogFile = null;
//            }
//        } catch (IOException ioe) {
//            LOGGER.info("Unable to preserve log file (grid job id "
//                    + job.getJobID() + "): " + ioe.getMessage(), ioe);
//            newLogFile = null;
//        }
//
//        try {
//            FileUtil.copy(outFile, newOutFile);
//        } catch (IOException e) {
//            LOGGER.info("Unable to preserve stdout file (grid job id "
//                    + job.getJobID() + "): " + e.getMessage(), e);
//            newOutFile = null;
//        }
//
//        try {
//            FileUtil.copy(errFile, newErrFile);
//        } catch (IOException e) {
//            LOGGER.info("Unable to preserve std err file (grid job id "
//                    + job.getJobID() + "): " + e.getMessage(), e);
//            newErrFile = null;
//        }
//
//        synchronized (m_fileMapMutex) {
//            m_logFile.put(jobIdx, newLogFile);
//            m_outFile.put(jobIdx, newOutFile);
//            m_errFile.put(jobIdx, newErrFile);
//        }
//
//        for (JobManagerView view : m_views) {
//            view.showFiles(jobIdx, newLogFile, newOutFile, newErrFile);
//        }
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean canSaveInternals() {
//        return m_hasViewContentToSave.get();
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public synchronized void saveInternals(final ReferencedFile refDir)
//            throws IOException {
//        if (!m_hasViewContentToSave.get()) {
//            return;
//        }
//
//        refDir.lock();
//        try {
//            File directory = refDir.getFile();
//            assert directory != null;
//            assert directory.exists() && directory.isDirectory();
//            assert directory.canWrite();
//
//            // copy stderr logs for all split jobs
//            for (Map.Entry<Integer, File> e : m_errFile.entrySet()) {
//                File errFile = e.getValue();
//                if (errFile == null || !errFile.exists()) {
//                    continue;
//                }
//                File dir = new File(directory, e.getKey().toString());
//                dir.mkdirs();
//                File errCopy = new File(dir, STDERR_FILENAME);
//                try {
//                    FileUtil.copy(errFile, errCopy);
//                } catch (final IOException ioe) {
//                    String msg = ioe.getMessage();
//                    if (msg == null) {
//                        msg = "<no details>";
//                    }
//                    LOGGER.error("Unable to save stderr to dir "
//                            + dir.getAbsolutePath() + ": " + msg, ioe);
//                }
//            }
//
//            // same for stdout
//            for (Map.Entry<Integer, File> e : m_outFile.entrySet()) {
//                File outFile = e.getValue();
//                if (outFile == null || !outFile.exists()) {
//                    continue;
//                }
//                File dir = new File(directory, e.getKey().toString());
//                dir.mkdirs();
//                File outCopy = new File(dir, STDOUT_FILENAME);
//                try {
//                    FileUtil.copy(outFile, outCopy);
//                } catch (final IOException ioe) {
//                    String msg = ioe.getMessage();
//                    if (msg == null) {
//                        msg = "<no details>";
//                    }
//                    LOGGER.error("Unable to save stdout to dir "
//                            + dir.getAbsolutePath() + ": " + msg, ioe);
//                }
//            }
//
//            // and for the KNIME log file
//            for (Map.Entry<Integer, File> e : m_logFile.entrySet()) {
//                File logFile = e.getValue();
//                if (logFile == null || !logFile.exists()) {
//                    continue;
//                }
//                File dir = new File(directory, e.getKey().toString());
//                dir.mkdirs();
//                File logCopy = new File(dir, KNIMELOG_FILENAME);
//                try {
//                    FileUtil.copy(logFile, logCopy);
//                } catch (final IOException ioe) {
//                    String msg = ioe.getMessage();
//                    if (msg == null) {
//                        msg = "<no details>";
//                    }
//                    LOGGER.error("Unable to save KNIME log file to dir "
//                            + dir.getAbsolutePath() + ": " + msg, ioe);
//                }
//            }
//
//            File viewSettingsFile =
//                    new File(refDir.getFile(), VIEWSETTINGS_FILENAME);
//            NodeSettings otherSettings = new NodeSettings("SGEViewSettings");
//
//            otherSettings.addBoolean(
//                    CFGKEY_OLDCONTENT, m_contentFromPrevRun.get());
//            otherSettings.addBoolean(CFGKEY_MULTIPLEFILES, m_showMultipleFiles);
//            otherSettings.saveToXML(new BufferedOutputStream(
//                    new FileOutputStream(viewSettingsFile)));
//        } finally {
//            refDir.unlock();
//        }
//
//    }
//
//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public synchronized void loadInternals(final ReferencedFile directory)
//            throws IOException {
//
//        m_saveDirectory = directory;
//        m_contentFromPrevRun.set(false);
//        m_showMultipleFiles = false;
//
//        File settingsFile =
//                new File(directory.getFile(), VIEWSETTINGS_FILENAME);
//        if (settingsFile.isFile() && settingsFile.canRead()) {
//            NodeSettingsRO viewSettings =
//                    NodeSettings.loadFromXML(new BufferedInputStream(
//                            new FileInputStream(settingsFile)));
//
//            try {
//                m_contentFromPrevRun.set(
//                        viewSettings.getBoolean(CFGKEY_OLDCONTENT));
//                m_showMultipleFiles =
//                        viewSettings.getBoolean(CFGKEY_MULTIPLEFILES);
//            } catch (InvalidSettingsException e) {
//                m_showMultipleFiles = true;
//                throw new RuntimeException("required view settings not found");
//            }
//        }
//
//    }
//
//    private void transferSavedFilesInMap(final ReferencedFile dir,
//            final String fileName, final Map<Integer, File> map) {
//        dir.lock();
//        try {
//            File directory = dir.getFile();
//            File[] subDirs = directory.listFiles();
//            for (File subdir : subDirs) {
//                if (!subdir.isDirectory()) {
//                    // our files are stored in subdirs
//                    continue;
//                }
//                Integer jobIdx;
//                try {
//                    jobIdx = Integer.parseInt(subdir.getName());
//                } catch (NumberFormatException nfe) {
//                    // this is not one of our dirs...
//                    continue;
//                }
//                File logFile = new File(subdir, fileName);
//                if (!logFile.isFile() || !logFile.canRead()) {
//                    continue;
//                }
//                map.put(jobIdx, logFile);
//            }
//        } finally {
//            dir.unlock();
//        }
//
//    }
//
//}
