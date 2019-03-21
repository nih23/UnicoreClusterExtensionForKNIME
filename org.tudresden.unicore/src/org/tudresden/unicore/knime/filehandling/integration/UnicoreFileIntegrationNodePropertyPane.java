package org.tudresden.unicore.knime.filehandling.integration;

import javax.swing.JFileChooser;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentBoolean;
import org.knime.core.node.defaultnodesettings.DialogComponentButtonGroup;
import org.knime.core.node.defaultnodesettings.DialogComponentFileChooser;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

public class UnicoreFileIntegrationNodePropertyPane extends DefaultNodeSettingsPane {

	UnicoreFileIntegrationNodePropertyPane()
	{
		addDialogComponent(new DialogComponentButtonGroup(new SettingsModelString(UnicoreFileIntegrationNodeModel.PropsUseMode, null), false, "Unicore Use Mode", UnicoreFileIntegrationNodeModel.unicoreUseModes ));
		 // following components are bordered
        createNewGroup("Local Data Integration");       
        addDialogComponent(new DialogComponentFileChooser(new SettingsModelString(UnicoreFileIntegrationNodeModel.PropsLocalDirectory, null), "2342",  JFileChooser.SAVE_DIALOG , true));
        closeCurrentGroup();
        // closes the prev group, opens a new one>
        
        createNewGroup("Remote Execution");        
        addDialogComponent(new DialogComponentBoolean(new SettingsModelBoolean(
		UnicoreFileIntegrationNodeModel.PropsS3Staging, false), "Data Staging"));
        closeCurrentGroup();
        
		
	}
}
