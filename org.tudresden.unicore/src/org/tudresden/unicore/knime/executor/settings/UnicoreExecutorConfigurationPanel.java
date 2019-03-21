package org.tudresden.unicore.knime.executor.settings;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.File;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JFileChooser;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPasswordField;
import javax.swing.JRadioButton;
import javax.swing.JTextField;
import javax.swing.event.DocumentEvent;

import org.eclipse.jface.preference.DirectoryFieldEditor;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.swt.widgets.Composite;
import org.knime.cluster.ClusterConfigurationPanel;

public class UnicoreExecutorConfigurationPanel extends JPanel {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4333438025523980733L;
	private final UnicoreExecutorConfiguration m_configuration;

	/*
	 * UNICORE PARAMETERS
	 */
	private final JTextField m_unicore_resources_runtime = new JTextField(10);
	private final JTextField m_unicore_resources_memory = new JTextField(10);
	private final JTextField m_unicore_resources_nodes = new JTextField(10);   
	private final JTextField m_unicore_resources_cpusPerNode = new JTextField(10);
	private final JTextField m_unicore_gateway = new JTextField(15);
	private final JTextField m_unicore_site = new JTextField(10);
	private final JTextField m_unicore_storage_sink = new JTextField(15);
	private final JTextField m_unicore_username = new JTextField(15);
	private final JPasswordField m_unicore_password = new JPasswordField(15);
	
	private final JTextField m_clusterShared = new JTextField(15);

	// only when loading is zero we honor change events
	private final AtomicInteger m_loading = new AtomicInteger(0);

	Box prefsBox = Box.createHorizontalBox();

	/**
	 * @param nodeSplitType type of splitting permitted by the underlying node
	 */
	public UnicoreExecutorConfigurationPanel(UnicoreExecutorConfiguration settings) {
		//super(settings);
		m_configuration = settings;

		setLayout(new GridBagLayout());
		GridBagConstraints gbc = new GridBagConstraints();
		gbc.insets = new Insets(5, 5, 5, 5);
		gbc.anchor = GridBagConstraints.NORTHWEST;
		gbc.fill = GridBagConstraints.BOTH;
		gbc.weightx = 0;
		gbc.gridx = 0;
		gbc.gridy = 0;
		
		transferSettingsIntoComponents();

		MyDocumentListener mdl = new MyDocumentListener();

		gbc.gridx = 0;
		add(new JLabel("Gateway  "), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_gateway, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Username  "), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_username, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Password  "), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_password, gbc);
		m_unicore_password.setEchoChar('*');
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Queue"), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_site, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Storage sink"), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_storage_sink, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("No. Nodes  "), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_resources_nodes, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("No. CPUs per Node  "), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_resources_cpusPerNode, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Memory"), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_resources_memory, gbc);
		gbc.gridy++;
		
		gbc.gridx = 0;
		add(new JLabel("Runtime"), gbc);
		gbc.gridx++;
		gbc.weightx = 1;
		add(m_unicore_resources_runtime, gbc);
		gbc.gridy++;

		m_unicore_storage_sink.getDocument().addDocumentListener(mdl);
		m_unicore_username.getDocument().addDocumentListener(mdl);
		m_unicore_password.getDocument().addDocumentListener(mdl);

		m_unicore_resources_runtime.getDocument().addDocumentListener(mdl);
		m_unicore_resources_nodes.getDocument().addDocumentListener(mdl);
		m_unicore_resources_cpusPerNode.getDocument().addDocumentListener(mdl);
		m_unicore_resources_memory.getDocument().addDocumentListener(mdl);
		m_unicore_gateway.getDocument().addDocumentListener(mdl);
		m_unicore_site.getDocument().addDocumentListener(mdl);
	}

	/**
	 * Transfers the currently entered values from this tab's components into
	 * the provided settings object. Does not change any other values in the
	 * settings object.
	 */
	private void transferComponentsValuesIntoSettings() {
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE, m_unicore_storage_sink.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME, m_unicore_resources_runtime.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES, m_unicore_resources_nodes.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE, m_unicore_resources_cpusPerNode.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY, m_unicore_gateway.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE, m_unicore_site.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY, m_unicore_resources_memory.getText());
		
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Username, m_unicore_username.getText());
		m_configuration.setSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Password, m_unicore_password.getText());	
	}

	/**
	 * Simply reads all values from the settings object and transfers them into
	 * the dialog's components.
	 *
	 * @param settings the settings values to display
	 */
	private void transferSettingsIntoComponents() {
		m_unicore_resources_runtime.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_RUNTIME));
		m_unicore_resources_nodes.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_NODES));
		m_unicore_resources_cpusPerNode.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_CPUSPERNODE));
		m_unicore_resources_memory.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_RESOURCES_MEMORY));
		m_unicore_gateway.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_GATEWAY));
		m_unicore_site.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_SITE));
		
		m_unicore_storage_sink.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_STORAGE));
		m_unicore_username.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Username));
		m_unicore_password.setText(m_configuration.getSetting(UnicoreJobManagerSettings.CFG_UnicoreJOB_Password));
	}

	private class MyDocumentListener implements javax.swing.event.DocumentListener {

		@Override
		public void insertUpdate(DocumentEvent e) {
			transferComponentsValuesIntoSettings();			
		}

		@Override
		public void removeUpdate(DocumentEvent e) {
			transferComponentsValuesIntoSettings();
		}

		@Override
		public void changedUpdate(DocumentEvent e) {
			transferComponentsValuesIntoSettings();
		}

	}

}
