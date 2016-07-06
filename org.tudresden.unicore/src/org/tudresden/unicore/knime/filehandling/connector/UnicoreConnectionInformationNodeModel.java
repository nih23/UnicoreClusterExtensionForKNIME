package org.tudresden.unicore.knime.filehandling.connector;

import java.io.File;
import java.io.IOException;

import org.knime.base.filehandling.remote.connectioninformation.node.ConnectionInformationNodeModel;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettings;
import org.tudresden.unicore.knime.executor.settings.UnicorePreferenceInitializer;
import org.tudresden.unicore.knime.filehandling.UnicoreConnectionInformation;

/**
 * This is the model implementation.
 *
 * @author Nico Hoffmann, Patrick Winter
 */
public class UnicoreConnectionInformationNodeModel extends ConnectionInformationNodeModel {

    private final Protocol m_protocol;

    private UnicoreConnectionInformationConfiguration m_configuration;

    static final String P_User = "prop_unicore_user";
    static final String P_Pass = "prop_unicore_password";
    static final String P_GW = "prop_unicore_gateway";
    static final String P_Queue = "prop_unicore_queue";
    static final String P_Site = "prop_unicore_site";
    
    private final SettingsModelString m_settings_user; // = new SettingsModelString(P_User, null);
    private final SettingsModelString m_settings_pass; // = new SettingsModelString(P_Pass, null);
    private final SettingsModelString m_settings_gw; // = new SettingsModelString(P_GW, null);
    private final SettingsModelString m_settings_queue; // = new SettingsModelString(P_Queue, null);
    private final SettingsModelString m_settings_site; // = new SettingsModelString(P_Site, null);
   
    /**
     * Constructor for the node model.
     *
     * @param protocol The protocol of this connection information model
     */
    public UnicoreConnectionInformationNodeModel(final Protocol protocol) {
    	super(protocol);
        m_protocol = protocol;
        
        UnicoreJobManagerSettings prefs = new UnicoreJobManagerSettings();
        UnicorePreferenceInitializer.getSettingsFromPreferences(prefs);
        
        
        m_settings_user = new SettingsModelString(P_User, prefs.getUsername());
        m_settings_pass = new SettingsModelString(P_Pass, prefs.getPassword());
        m_settings_gw = new SettingsModelString(P_GW, prefs.getGateway());
        m_settings_queue = new SettingsModelString(P_Queue, prefs.getDefaultSitename());
        m_settings_site = new SettingsModelString(P_Site, prefs.getStorage());

    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
    	return new PortObject[]{new UnicoreConnectionInformationPortObject(createSpec())};
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
        return new PortObjectSpec[]{createSpec()};
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void saveSettingsTo(final NodeSettingsWO settings) {
    		super.saveSettingsTo(settings);
    		
    		m_settings_user.saveSettingsTo(settings);
    		m_settings_pass.saveSettingsTo(settings);
    		m_settings_gw.saveSettingsTo(settings);
    		m_settings_queue.saveSettingsTo(settings);
    		m_settings_site.saveSettingsTo(settings);
    }
    
    private String getHostFromGW(String gw) {
    	String host = gw.replace("https://", "").replace("http://", "");
        host = host.substring(0, host.indexOf(":"));
        return host;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
        // retrieve information from knime preferences pane.
        UnicoreJobManagerSettings prefs = new UnicoreJobManagerSettings();
        UnicorePreferenceInitializer.getSettingsFromPreferences(prefs);
        
        final UnicoreConnectionInformationConfiguration config = new UnicoreConnectionInformationConfiguration(m_protocol, prefs.getRemoteRootDir(), prefs.getStorage());
        config.load(settings);
        config.setUser(prefs.getUsername());
        config.setPassword(prefs.getPassword());
        
        
        config.setHost(getHostFromGW(prefs.getGateway()));
        config.setAuthenticationmethod("password");
        m_configuration = config;
        
        
		m_settings_user.loadSettingsFrom(settings);
		m_settings_pass.loadSettingsFrom(settings);
		m_settings_gw.loadSettingsFrom(settings);
		m_settings_queue.loadSettingsFrom(settings);
		m_settings_site.loadSettingsFrom(settings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
        // nothing to validate..
    }

    /**
     * Create the spec, throw exception if no config available.
     *
     * @return ...
     * @throws InvalidSettingsException ...
     */
    public UnicoreConnectionInformationPortObjectSpec createSpec() throws InvalidSettingsException {
        if (m_configuration == null || m_configuration.getHost() == null) {
            throw new InvalidSettingsException("No configuration available");
        }
        
 
        // integrate settings into configuration
        m_configuration.setUser(m_settings_user.getStringValue());
        m_configuration.setPassword(m_settings_pass.getStringValue());
        m_configuration.setHost(getHostFromGW(m_settings_gw.getStringValue()));
        m_configuration.setAuthenticationmethod("password");
        m_configuration.setQueue(m_settings_queue.getStringValue());
        m_configuration.setGateway(m_settings_gw.getStringValue());
        m_configuration.setStorageSite(m_settings_site.getStringValue());
        
        
        final UnicoreConnectionInformation connectionInformation =
                m_configuration.getConnectionInformation(getCredentialsProvider());
        return new UnicoreConnectionInformationPortObjectSpec(connectionInformation);
    }

	@Override
	protected void loadInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		super.loadInternals(nodeInternDir, exec);
		
	}

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		super.saveInternals(nodeInternDir, exec);
		
	}

	@Override
	protected void reset() {
		super.reset();
	}
	
}
