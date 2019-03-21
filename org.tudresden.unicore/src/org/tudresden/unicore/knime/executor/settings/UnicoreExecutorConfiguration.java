package org.tudresden.unicore.knime.executor.settings;

import java.util.HashMap;

import javax.swing.JPanel;

import org.knime.cluster.api.AbstractClusterConfiguration;
import org.knime.cluster.api.ClusterHandler;
import org.knime.cluster.api.ClusterJobSubSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.config.ConfigRO;
import org.knime.core.node.config.ConfigWO;
import org.tudresden.unicore.knime.UnicoreExecutorHandler;

public class UnicoreExecutorConfiguration extends AbstractClusterConfiguration {

	private HashMap<String, String> m_settings;
	
	public UnicoreExecutorConfiguration() {
		m_settings = new HashMap<String, String>();
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
	
	@Override
	public ClusterHandler createClusterHandler() {
		return new UnicoreExecutorHandler(this);
	}

	@Override
	public String getIntegrationName() {
		return "Unicore Executor";
	}
	
	@Override
	public ClusterJobSubSettings createExecutorNodeSettings() {
		return new UnicoreJobManagerSettings();
	}
	
	@Override
	public void load(final ConfigRO config) throws InvalidSettingsException {
		super.load(config);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_Username);
		validateAndAddConfig(config, UnicoreJobManagerSettings.CFG_UnicoreJOB_Password);
	}
	
	public void validateAndAddConfig(ConfigRO config, String settingName) throws InvalidSettingsException {
		if(config.containsKey(settingName)) {
			String settingValue = config.getString(settingName);
			if(settingValue == null) {
				settingValue = "";
			}
			setSetting(settingName, settingValue);
		}
	}

	@Override
	public void save(final ConfigWO config) throws InvalidSettingsException {
		super.save(config);
		
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_Password, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Password));
		config.addString(UnicoreJobManagerSettings.CFG_UnicoreJOB_Username, getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Username));
	}

	@Override
	public JPanel createConfigurationPanel() {
		return new UnicoreExecutorConfigurationPanel(this);
	}

}
