package org.tudresden.unicore.knime.executor.settings;

import java.io.File;

import org.eclipse.jface.preference.BooleanFieldEditor;
import org.eclipse.jface.preference.DirectoryFieldEditor;
import org.eclipse.jface.preference.FieldEditorPreferencePage;
import org.eclipse.jface.preference.RadioGroupFieldEditor;
import org.eclipse.jface.preference.StringFieldEditor;
import org.eclipse.swt.widgets.Composite;
import org.eclipse.swt.widgets.Control;
import org.eclipse.ui.IWorkbench;
import org.eclipse.ui.IWorkbenchPreferencePage;
import org.knime.cluster.executor.settings.ClusterJobExecSettings;
import org.knime.cluster.executor.settings.ClusterJobExecSettings.DeleteTempFilePolicy;

/**
 * KNIME preference page for global unicore settings
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */

public class UnicorePreferencePage extends FieldEditorPreferencePage implements
IWorkbenchPreferencePage {

	private StringFieldEditor m_knimeExe;
    private StringFieldEditor m_knimeArgs;
    private StringFieldEditor m_localShared;
    private StringFieldEditor m_clusterShared;
    private StringFieldEditor m_unicore_username;
    private StringFieldEditor m_unicore_password;
    private StringFieldEditor m_unicore_gateway;  
    private StringFieldEditor m_unicore_storage;    
    private StringFieldEditor m_unicore_default_sitename;   
    private StringFieldEditor m_unicore_resources_runtime;    
    private StringFieldEditor m_unicore_resources_memory;    
    private StringFieldEditor m_unicore_resources_nodes;    
    private StringFieldEditor m_unicore_resources_cpusPerNode;

    /**
     * Constructor
     */
    public UnicorePreferencePage() {
        super(GRID);
    }


    /**
     * {@inheritDoc}
     */
    @Override
    protected Control createContents(final Composite parent) {
            return super.createContents(parent);
    }

    /**
     * Creates the field editors. Field editors are abstractions of the common
     * GUI blocks needed to manipulate various types of preferences. Each field
     * editor knows how to save and restore itself.<br />
     * The page created should look somewhat similar to the node dialog pane of
     * the job manager (see {@link SGEJobManagerSettingsPanel})
     */
    @Override
    public void createFieldEditors() {
        m_knimeExe =
                new SFE(UnicorePreferenceInitializer.PREF_KNIME_EXEC,
                        "KNIME exe (cluster node absolute path):",
                        getFieldEditorParent());
        m_knimeArgs =
                new SFE(UnicorePreferenceInitializer.PREF_KNIME_ARGS,
                        "Command line arguments for KNIME:",
                        getFieldEditorParent());
        
        m_unicore_username =
        		new SFE(UnicorePreferenceInitializer.PREF_UNICORE_USERNAME,
                        "Username for Unicore:",
                        getFieldEditorParent());
        
        m_unicore_password =
        		new SFE(UnicorePreferenceInitializer.PREF_UNICORE_PASSWORD,
                        "Password for Unicore:",
                        getFieldEditorParent());
        m_unicore_password.getTextControl(getFieldEditorParent()).setEchoChar('*') ;
        
        m_unicore_gateway =
        		new SFE(UnicorePreferenceInitializer.PREF_UNICORE_GATEWAY,
                        "Gateway for Unicore:",
                        getFieldEditorParent());
        
        //TODO: only provide availiable sites and let user chose
        m_unicore_storage =
        		new SFE(UnicorePreferenceInitializer.PREF_UNICORE_STORAGE,
                        "Storage Resource for Unicore:",
                        getFieldEditorParent());
        
        //TODO: only provide availiable sites and let user chose
        m_unicore_default_sitename =
        		new SFE(UnicorePreferenceInitializer.PREF_UNICORE_DEFAULT_SITENAME,
                        "Default sitename for Unicore:",
                        getFieldEditorParent());
        
        m_localShared =
                new DFE(UnicorePreferenceInitializer.PREF_LOCAL_SHARED,
                        "Local temporary directory:",
                        getFieldEditorParent());
        
        m_clusterShared =
                new SFE(UnicorePreferenceInitializer.PREF_CLUSTER_SHARED,
                        "Cluster temporary directory (relative to storage):",
                        getFieldEditorParent());

        m_unicore_resources_nodes = new SFE(UnicorePreferenceInitializer.PREF_UNICORE_RESOURCES_NODES,
                "HPC Nodes",
                getFieldEditorParent());
        
        m_unicore_resources_cpusPerNode = new SFE(UnicorePreferenceInitializer.PREF_UNICORE_RESOURCES_CPUsPerNode,
                "CPUs per Node",
                getFieldEditorParent());
        
        m_unicore_resources_memory = new SFE(UnicorePreferenceInitializer.PREF_UNICORE_RESOURCES_MEMORY,
                "Memory [K,M,G]",
                getFieldEditorParent());
        
        m_unicore_resources_runtime = new SFE(UnicorePreferenceInitializer.PREF_UNICORE_RESOURCES_RUNTIME,
                "Runtime [s,min,h,d]",
                getFieldEditorParent());
      
        
        addField(m_knimeExe);

        // KNIME Args
        addField(m_knimeArgs);
       
        // global Unicore preferences 
        addField(m_unicore_username);
        addField(m_unicore_password);
        addField(m_unicore_gateway);
        addField(m_unicore_storage);
        addField(m_unicore_default_sitename);
        
        // Unicore job description
        addField(m_unicore_resources_runtime);
        addField(m_unicore_resources_nodes);
        addField(m_unicore_resources_cpusPerNode);
        addField(m_unicore_resources_memory);
        

        // Shared Dir (local) -> Browse Button DirectoryEditor
        addField(m_localShared);

        // Shared Dir (cluster)
        addField(m_clusterShared);

        // Export client preferences onto grid machines
        addField(new BooleanFieldEditor(
        		UnicorePreferenceInitializer.PREF_EXPORT_CLIENT_PREFERENCES,
                "Export client preferences onto grid", getFieldEditorParent()));

        addField(new RadioGroupFieldEditor(
        		UnicorePreferenceInitializer.PREF_INVOKE_SHELL,
                "Grid job invocation in", 2, new String[][]{
                        {
                        UnicoreJobManagerSettings.InvokeScriptShell.bash
                                        .name(),
                        UnicoreJobManagerSettings.InvokeScriptShell.bash
                                        .name()},
                        {
                        UnicoreJobManagerSettings.InvokeScriptShell.tcsh
                                        .name(),
                         UnicoreJobManagerSettings.InvokeScriptShell.tcsh
                                        .name()}}, getFieldEditorParent(), true));

        // delete temp file policy
        addField(new RadioGroupFieldEditor(
        		UnicorePreferenceInitializer.PREF_DELETE_TEMP_FILE_POLICY,
                "Delete temp files", 2, new String[][]{
                        {"Always", DeleteTempFilePolicy.Always.name()},
                        {"If succeeds",
                                DeleteTempFilePolicy.IfJobSucceeds.name()},
                        {"Never", DeleteTempFilePolicy.Never.name()}},
                getFieldEditorParent()));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void init(final IWorkbench workbench) {
        setPreferenceStore(UnicorePreferenceInitializer.getUnicorePreferenceStore());
    }

    /**
     * Checks the settings and returns null if everything is valid, otherwise an
     * error message.
     *
     * @return null, if settings are valid, an error message otherwise
     */
    private String checkValues() {
    	UnicoreJobManagerSettings s = new UnicoreJobManagerSettings();
        s.setUsePreferences(false);
        s.setRemoteKnimeExecutable(m_knimeExe.getStringValue());
        s.setCustomKnimeArguments(ClusterJobExecSettings
                .splitArgumentString(m_knimeArgs.getStringValue()));
        /*s.setDefaultSitename(m_unicore_default_sitename.getStringValue());
        s.setUsername(m_unicore_username.getStringValue());
        s.setPassword(m_unicore_password.getStringValue());
        s.setGateway(m_unicore_gateway.getStringValue());*/
        s.setLocalRootDir(new File(m_localShared.getStringValue()));
        s.setRemoteRootDir(m_clusterShared.getStringValue());
        return s.getStatusMsg();
    }

    /**
     * With the default components the pref page in Eclipse has an odd error
     * message behavior. If you have errors in multiple fields, and you correct
     * one of them - the error message disappears, but OK stays disabled. Only
     * after modifying the one with the error - and you have to guess which one
     * it is - you see the next error message.<br>
     * This component returns always <code>true</code> indicating an okay state.
     * It sets the error message according to all values of this pref page and
     * sets the validity of the pref page accordingly. This way we can use the
     * checking routine in our settings object. Which is good.
     *
     * @author ohl, University of Konstanz
     */
    private final class SFE extends StringFieldEditor {
  	
        /**
         * A new StringFieldEditor
         *
         * @param prefName preference name
         * @param label text label
         * @param parent parent
         */
        SFE(final String prefName, final String label, final Composite parent) {
            super(prefName, label, parent);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean checkState() {
            String errMsg = UnicorePreferencePage.this.checkValues();
            UnicorePreferencePage.this.setValid(errMsg == null);
            if (errMsg != null) {
            	UnicorePreferencePage.this.setErrorMessage(errMsg);
            } else {
            	UnicorePreferencePage.this.setErrorMessage(null);
            }
            return true;
        }
    }

    private final class DFE extends DirectoryFieldEditor {

        /**
         * @param name
         * @param labelText
         * @param parent
         */
        public DFE(final String name, final String labelText,
                final Composite parent) {
            super(name, labelText, parent);
            // that's too late. The super instantiates the component and
            // registers listeners depending on the validate strategy.
            setValidateStrategy(VALIDATE_ON_KEY_STROKE);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void setValidateStrategy(final int value) {
            // I want ON_KEY_STROKE. Nothing else.
            // ON_FOCUS_LOST clears the error message - which I don't like.
            super.setValidateStrategy(VALIDATE_ON_KEY_STROKE);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        protected boolean checkState() {
            String errMsg = UnicorePreferencePage.this.checkValues();
            UnicorePreferencePage.this.setValid(errMsg == null);
            if (errMsg != null) {
                UnicorePreferencePage.this.setErrorMessage(errMsg);
            } else {
                UnicorePreferencePage.this.setErrorMessage(null);
            }
            return true;
        }

    }

}
