package org.tudresden.unicore.knime.filehandling.connector;

import java.util.LinkedList;
import java.util.List;

import javax.swing.JComboBox;

import org.knime.cluster.ClusterConfigurationRepository;
import org.knime.cluster.ClusterConfigurationRepositoryRO;
import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentPasswordField;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.DialogComponentStringSelection;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * property panel for unicore connector
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreConnectionInformationNodePropertyPane extends DefaultNodeSettingsPane {
	
	//private final JComboBox<String> m_configuration;
	
	UnicoreConnectionInformationNodePropertyPane()
	{
		ClusterConfigurationRepositoryRO ccr = ClusterConfigurationRepository.getRepository();
        createNewGroup("Unicore Settings");  
        
        List<String> configs = ccr.getConfigurationNames();
        SettingsModelString stringModel = new SettingsModelString(UnicoreConnectionInformationNodeModel.P_ClusterConfig,configs.get(0));
        addDialogComponent(new DialogComponentStringSelection(stringModel, "Cluster profile", configs));
        closeCurrentGroup();
	}

}
