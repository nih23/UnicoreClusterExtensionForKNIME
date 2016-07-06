package org.tudresden.unicore.knime.executor.settings;

import java.awt.BorderLayout;

import javax.swing.JTabbedPane;

import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.NodeContainer.NodeContainerSettings.SplitType;
import org.knime.core.node.workflow.NodeExecutionJobManagerPanel;

/**
 * (wrapped)metanode settings panel for remote job settings 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */

public class UnicoreJobManagerSettingsPanel extends NodeExecutionJobManagerPanel {

	private final UnicoreSettingsChunkPanel m_chunkPanel;
    private final UnicoreSettingsPrefPanel m_prefsPanel;
    private final SplitType m_nodeSplitType;

    /**
     * @param nodeSplitType type of splitting permitted by the underlying node
     */
    public UnicoreJobManagerSettingsPanel(final SplitType nodeSplitType) {

        setLayout(new BorderLayout());

        m_nodeSplitType = nodeSplitType;
        m_chunkPanel = new UnicoreSettingsChunkPanel(m_nodeSplitType);
        m_prefsPanel = new UnicoreSettingsPrefPanel();
        JTabbedPane tabs = new JTabbedPane();
        tabs.addTab("Data Partitioning", m_chunkPanel);
        tabs.addTab("Remote Execution", m_prefsPanel);
        tabs.setSelectedIndex(0);
        
        add(tabs, BorderLayout.EAST);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void loadSettings(final NodeSettingsRO settings) {
        UnicoreJobManagerSettings unicoreSettings;
        try {
            unicoreSettings = new UnicoreJobManagerSettings(settings);
        } catch (InvalidSettingsException e) {
            unicoreSettings = new UnicoreJobManagerSettings();
        }
        m_chunkPanel.loadSettings(unicoreSettings);
        m_prefsPanel.loadSettings(unicoreSettings);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateInputSpecs(final PortObjectSpec[] inSpecs) {
        m_chunkPanel.updateInputSpecs(inSpecs);
        m_prefsPanel.updateInputSpecs(inSpecs);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void saveSettings(final NodeSettingsWO settings)
            throws InvalidSettingsException {

        UnicoreJobManagerSettings s = new UnicoreJobManagerSettings();
        m_prefsPanel.saveSettings(s);
        m_chunkPanel.saveSettings(s);
        String errMsg = s.getStatusMsg();
        if (errMsg != null) {
            throw new InvalidSettingsException(errMsg);
        }
        s.save(settings);
    }

}
