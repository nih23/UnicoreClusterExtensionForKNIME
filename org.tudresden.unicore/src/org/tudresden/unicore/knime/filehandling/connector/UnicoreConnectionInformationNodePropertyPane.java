package org.tudresden.unicore.knime.filehandling.connector;

import org.knime.core.node.defaultnodesettings.DefaultNodeSettingsPane;
import org.knime.core.node.defaultnodesettings.DialogComponentPasswordField;
import org.knime.core.node.defaultnodesettings.DialogComponentString;
import org.knime.core.node.defaultnodesettings.SettingsModelString;

/**
 * property panel for unicore connector
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreConnectionInformationNodePropertyPane extends DefaultNodeSettingsPane {
	
	UnicoreConnectionInformationNodePropertyPane()
	{
        createNewGroup("Unicore Settings");            
        addDialogComponent(new DialogComponentString(new SettingsModelString(UnicoreConnectionInformationNodeModel.P_User, null), "Username"));
        addDialogComponent(new DialogComponentPasswordField(new SettingsModelString(UnicoreConnectionInformationNodeModel.P_Pass, null), "Password"));
        addDialogComponent(new DialogComponentString(new SettingsModelString(UnicoreConnectionInformationNodeModel.P_GW, null), "Gateway"));
        addDialogComponent(new DialogComponentString(new SettingsModelString(UnicoreConnectionInformationNodeModel.P_Queue, null), "Queue"));
        addDialogComponent(new DialogComponentString(new SettingsModelString(UnicoreConnectionInformationNodeModel.P_Site, null), "Storage Site"));
        closeCurrentGroup();
	}

}
