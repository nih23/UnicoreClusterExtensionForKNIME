package org.tudresden.unicore.knime.executor.settings;

import org.knime.cluster.executor.settings.ClusterJobSplitSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;


/**
 * settings container for execution of remote jobs
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreJobManagerSettings extends ClusterJobSplitSettings {
	private static final String CFG_UPPERCLASS_CONFIG = "SuperClassSettings";
	private static final String CFG_UnicoreJOB_STORAGE = "Unicore_NativeJobStorage";
	private static final String CFG_UnicoreJOB_RESOURCES_RUNTIME = "Unicore_Resources_Runtime";
	private static final String CFG_UnicoreJOB_RESOURCES_MEMORY = "Unicore_Resources_Memory";
	private static final String CFG_UnicoreJOB_RESOURCES_NODES = "Unicore_Resources_Nodes";
	private static final String CFG_UnicoreJOB_RESOURCES_CPUSPERNODE = "Unicore_Resources_CPUsPerNode";
	private static final String CFG_UnicoreJOB_GATEWAY = "Unicore_Gateway";
	private static final String CFG_UnicoreJOB_SITE = "Unicore_Site";

	private String m_unicoreUsername;
	private String m_unicorePassword;
	private String m_unicoreGateway;
	private String m_unicoreDefaultSitename;
	private String m_unicoreStorage;
	private String m_unicoreResourcesRuntime;
	private String m_unicoreResourcesMemory;
	private String m_unicoreResourcesNodes;
	private String m_unicoreResourcesCPUsPerNode;

	/**
	 * Default constructor with default settings, possibly invalid settings.
	 */
	public UnicoreJobManagerSettings() {
		m_unicoreDefaultSitename = "";
		m_unicoreGateway = "";
		m_unicorePassword = "";
		m_unicoreUsername = "";
		m_unicoreStorage = "default_storage";
		m_unicoreResourcesRuntime = "10min";
		m_unicoreResourcesMemory = "4G";
		m_unicoreResourcesNodes = "1";
		m_unicoreResourcesCPUsPerNode = "12";
	}

	/**
	 * Creates a new settings object with values from the object passed.
	 *
	 * @param settings object with the new values to set
	 * @throws InvalidSettingsException if settings object is invalid
	 */
	public UnicoreJobManagerSettings(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		super(getSuperConfig(settings));
		/*m_unicoreUsername = settings.getString(CFG_UnicoreJOB_USERNAME);
	        m_unicorePassword = settings.getString(CFG_UnicoreJOB_PASSWORD);
	        m_unicoreGateway = settings.getString(CFG_UnicoreJOB_GATEWAY);
	        m_unicoreDefaultSitename = settings.getString(CFG_UnicoreJOB_DEFAULT_SITENAME);
	        m_unicoreStorage = settings.getString(CFG_UnicoreJOB_STORAGE);*/
		m_unicoreResourcesMemory = settings.getString(CFG_UnicoreJOB_RESOURCES_MEMORY);
		m_unicoreResourcesCPUsPerNode = settings.getString(CFG_UnicoreJOB_RESOURCES_CPUSPERNODE);
		m_unicoreResourcesNodes = settings.getString(CFG_UnicoreJOB_RESOURCES_NODES);
		m_unicoreResourcesRuntime = settings.getString(CFG_UnicoreJOB_RESOURCES_RUNTIME);
		m_unicoreGateway = settings.getString(CFG_UnicoreJOB_GATEWAY);
		m_unicoreDefaultSitename = settings.getString(CFG_UnicoreJOB_SITE);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	public void save(final NodeSettingsWO settings) {
		super.save(settings.addNodeSettings(CFG_UPPERCLASS_CONFIG));
		/*settings.addString(CFG_UnicoreJOB_USERNAME, m_unicoreUsername);
	        settings.addString(CFG_UnicoreJOB_PASSWORD, m_unicorePassword);
	        settings.addString(CFG_UnicoreJOB_GATEWAY, m_unicoreGateway);
	        settings.addString(CFG_UnicoreJOB_DEFAULT_SITENAME, m_unicoreDefaultSitename);*/
		settings.addString(CFG_UnicoreJOB_STORAGE, m_unicoreStorage);
		settings.addString(CFG_UnicoreJOB_RESOURCES_CPUSPERNODE, m_unicoreResourcesCPUsPerNode);
		settings.addString(CFG_UnicoreJOB_RESOURCES_MEMORY, m_unicoreResourcesMemory);
		settings.addString(CFG_UnicoreJOB_RESOURCES_NODES, m_unicoreResourcesNodes);
		settings.addString(CFG_UnicoreJOB_RESOURCES_RUNTIME, m_unicoreResourcesRuntime);
		settings.addString(CFG_UnicoreJOB_GATEWAY, m_unicoreGateway);
		settings.addString(CFG_UnicoreJOB_SITE, m_unicoreDefaultSitename);
		
	}


	/**
	 * Earlier versions stored all settings in a flat config object. For
	 * backward compatibility check here.
	 *
	 * @param settings object with settings values
	 * @return the config for the super class to read its values from
	 * @throws InvalidSettingsException if settings are invalid
	 */
	private static NodeSettingsRO getSuperConfig(final NodeSettingsRO settings)
			throws InvalidSettingsException {
		if (settings.containsKey(CFG_UPPERCLASS_CONFIG)) {
			return settings.getNodeSettings(CFG_UPPERCLASS_CONFIG);
		} else {
			return settings;
		}
	}

	/**
	 * Returns null if the settings are valid. Otherwise a user message telling
	 * which settings are incorrect and why. If the "usePreferences" flag is set
	 * the values that are determined by the preference page are not checked.
	 *
	 * @return an error message if settings are invalid, of null, if everything
	 *         is alright
	 */
	public String getStatusMsg() {

		if (!usePreferences()) {
			// if we are to use prefs - most settings are ignored.
			if (getRemoteKnimeExecutable() == null
					|| getRemoteKnimeExecutable().trim().isEmpty()) {
				return "The path to the KNIME executable is not set.";
			}

			if (getLocalRootDir() == null) {
				return "The local path to the shared temp "
						+ "directory is not specified.";
			}
			if (!getLocalRootDir().isDirectory()
					|| !getLocalRootDir().canWrite()) {
				return "The local path to the shared directory must specify "
						+ "an existing directory with write permissions";
			}
		}
		if (splitExecution()) {
			if (!useChunkSize() && getNumberOfChunks() <= 0) {
				return "For split execution, the number of chunks "
						+ "must be larger than zero.";
			}
			if (useChunkSize() && getNumOfRowsPerChunk() <= 0) {
				return "For split execution, the size of the input "
						+ "tables must be a positive number.";
			}
			if (getSplitPortIdx() < 0) {
				return "The index of the port to split can't be negative";
			}
		}

		return null;
	}

	public String getStorage() {
		return m_unicoreStorage;
	}

	public void setStorage(String storage) {
		m_unicoreStorage = storage;
	}

	public String getJobRuntime() {
		return m_unicoreResourcesRuntime;
	}

	public void setJobRuntime(String runtime) {
		m_unicoreResourcesRuntime = runtime;
	}

	public String getJobMemory() {
		return m_unicoreResourcesMemory;
	}

	public void setJobMemory(String memory) {
		m_unicoreResourcesMemory = memory;
	}

	public String getJobNodes() {
		return m_unicoreResourcesNodes;
	}

	public void setJobNodes(String nodes) {
		m_unicoreResourcesNodes = nodes;
	}

	public String getJobCPUsPerNode() {
		return m_unicoreResourcesCPUsPerNode;
	}

	public void setJobCPUsPerNode(String cpusPerNode) {
		m_unicoreResourcesCPUsPerNode = cpusPerNode;
	}

	public String getDefaultSitename() {
		return m_unicoreDefaultSitename;
	}

	public void setDefaultSitename(final String defaultSitename) {
		m_unicoreDefaultSitename = defaultSitename;
	}

	public String getUsername() {
		return m_unicoreUsername;
	}

	public void setUsername(final String username) {
		m_unicoreUsername = username;
	}

	public String getPassword() {
		return m_unicorePassword;
	}

	public void setPassword(final String password) {
		m_unicorePassword = password;
	}

	public String getGateway() {
		return m_unicoreGateway;
	}

	public void setGateway(final String gateway) {
		m_unicoreGateway = gateway;
	}

	/**
	 * {@inheritDoc}
	 * <p>
	 * The shared dir SGE executor returns the local path, if no remote path was
	 * specified.
	 */
	@Override
	public String getRemoteRootDir() {
		if (super.getRemoteRootDir() == null
				|| super.getRemoteRootDir().isEmpty()) {
			return getLocalRootDir().getAbsolutePath();
		}
		return super.getRemoteRootDir();
	}
}
