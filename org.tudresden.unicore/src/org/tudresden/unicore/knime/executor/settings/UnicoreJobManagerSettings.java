package org.tudresden.unicore.knime.executor.settings;

import java.util.HashMap;

import org.knime.cluster.api.ClusterJobSubSettingsPanel;
import org.knime.cluster.api.ExecutorParametersNodeSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;

/**
 * settings container for execution of remote jobs
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreJobManagerSettings extends ExecutorParametersNodeSettings {
	public static final String CFG_UPPERCLASS_CONFIG = "SuperClassSettings";
	
	public static final String CFG_UnicoreJOB_REMOTE_PATH = "Unicore_NativeJobStorage";
	public static final String CFG_UnicoreJOB_STORAGE = "Unicore_NativeJobStorage";
	public static final String CFG_UnicoreJOB_RESOURCES_RUNTIME = "Unicore_Resources_Runtime";
	public static final String CFG_UnicoreJOB_RESOURCES_MEMORY = "Unicore_Resources_Memory";
	public static final String CFG_UnicoreJOB_RESOURCES_NODES = "Unicore_Resources_Nodes";
	public static final String CFG_UnicoreJOB_RESOURCES_CPUSPERNODE = "Unicore_Resources_CPUsPerNode";
	public static final String CFG_UnicoreJOB_GATEWAY = "Unicore_Gateway";
	public static final String CFG_UnicoreJOB_SITE = "Unicore_Site";
	public static final String CFG_UnicoreJOB_Username = "Unicore_Username";
	public static final String CFG_UnicoreJOB_Password = "Unicore_Password";
	
	private HashMap<String, String> m_settings;
	
	/**
	 * Default constructor with default settings, possibly invalid settings.
	 */
	public UnicoreJobManagerSettings() {
		super("unicoreSettings");
		m_settings = new HashMap<String, String>();
		setSetting(CFG_UnicoreJOB_REMOTE_PATH, "/");
	}

	public void setSetting(String name, String value) {
		m_settings.put(name,value);
	}
	
	public String getSetting(String name) {
		if(m_settings.containsKey(name)) {
			return m_settings.get(name);
		}
		return null;
	}

	/**
	 * {@inheritDoc}
	 * @throws InvalidSettingsException 
	 */
	@Override
	public void save(NodeSettingsWO settings) throws InvalidSettingsException {
		super.save(settings.addNodeSettings(CFG_UPPERCLASS_CONFIG));	
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
		return null;
//			// if we are to use prefs - most settings are ignored.
//			if (getRemoteKnimeExecutable() == null
//					|| getRemoteKnimeExecutable().trim().isEmpty()) {
//				return "The path to the KNIME executable is not set.";
//			}
//
//			if (getLocalRootDir() == null) {
//				return "The local path to the shared temp "
//						+ "directory is not specified.";
//			}
//			if (!getLocalRootDir().isDirectory()
//					|| !getLocalRootDir().canWrite()) {
//				return "The local path to the shared directory must specify "
//						+ "an existing directory with write permissions";
//			}
//
//		if (splitExecution()) {
//			if (!useChunkSize() && getNumberOfChunks() <= 0) {
//				return "For split execution, the number of chunks "
//						+ "must be larger than zero.";
//			}
//			if (useChunkSize() && getNumOfRowsPerChunk() <= 0) {
//				return "For split execution, the size of the input "
//						+ "tables must be a positive number.";
//			}
//			if (getSplitPortIdx() < 0) {
//				return "The index of the port to split can't be negative";
//			}
//		}
//
//		return null;
	}

	@Override
	public ClusterJobSubSettingsPanel getSettingsPanel() {
		return new UnicoreJobManagerSettingsPanel(this);
	}
}
