package org.tudresden.unicore.knime.executor.settings;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.io.File;
import java.util.Collection;
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
import javax.swing.JRadioButton;
import javax.swing.JTextField;

import org.knime.cluster.executor.settings.ClusterJobExecSettings.DeleteTempFilePolicy;
import org.knime.cluster.executor.settings.ClusterJobExecSettings.InvokeScriptShell;
import org.knime.cluster.executor.settings.ClusterJobSplitSettings;
import org.knime.core.node.port.PortObjectSpec;


/**
 * (wrapped)metanode settings panel to alter global unicore settings
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */

public class UnicoreSettingsPrefPanel extends JPanel {
    private UnicoreJobManagerSettings m_lastTabSettings = null;

	
	private final JCheckBox m_usePreferences = new JCheckBox();

    private final JButton m_setToDefaultVals = new JButton("Set to defaults");

    private final JTextField m_knimePath = new JTextField(30);

    private final JButton m_knimeBrowseBtn = new JButton("Browse...");

    private final JTextField m_knimeArgs = new JTextField(15);

    //private final JTextField m_nativeArgs = new JTextField(30);

    private final JTextField m_localShared = new JTextField(15);
    
    /*
     * UNICORE PARAMETERS
     */
    private final JTextField m_unicore_resources_runtime = new JTextField(10);

    private final JTextField m_unicore_resources_memory = new JTextField(10);

    private final JTextField m_unicore_resources_nodes = new JTextField(10);
    
    private final JTextField m_unicore_resources_cpusPerNode = new JTextField(10);


    private final JButton m_localBrowseBtn = new JButton("Browse...");

    private final JTextField m_clusterShared = new JTextField(15);

    private final JCheckBox m_useClientPreferencesOnGrid =
            new JCheckBox("Export client preferences to grid");

    private final JRadioButton m_delTempFilesAlways =
            new JRadioButton("Always");

    private final JRadioButton m_delTempFilesIfSucceeds =
            new JRadioButton("If Succeeds");

    private final JRadioButton m_delTempFilesNever = new JRadioButton("Never");

    private final JRadioButton m_invokeShellBash = new JRadioButton("bash");

    private final JRadioButton m_invokeShellTcsh = new JRadioButton("tcsh");

   // private SGEJobManagerSettings m_lastTabSettings = null;

    // only when loading is zero we honor change events
    private final AtomicInteger m_loading = new AtomicInteger(0);

    /**
     * Creates a new tab.
     */
    UnicoreSettingsPrefPanel() {
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        m_loading.incrementAndGet();

        Box prefsBox = Box.createHorizontalBox();
        prefsBox.add(Box.createVerticalStrut(15));
        m_usePreferences
                .setText("use default values from the Unicore preference page");
        m_usePreferences.addItemListener(new ItemListener() {
            public void itemStateChanged(final ItemEvent e) {
                if (m_loading.get() == 0) {
                    // only react to changes if we are not loading settings
                    usePrefsChanged();
                }
            }
        });
        m_setToDefaultVals.setToolTipText("Transfers values from preference"
                + " page into dialog");
        m_setToDefaultVals.addActionListener(new ActionListener() {
            public void actionPerformed(final ActionEvent e) {
                setDefaultValues();
            }
        });
        prefsBox.add(m_usePreferences);
        prefsBox.add(Box.createHorizontalGlue());
        prefsBox.add(m_setToDefaultVals);

        Box borderedBox = Box.createVerticalBox();
        borderedBox.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(),
                "Node specific settings (overriding global preferences)"));
        // KNIME Path
        Box knimeBox = Box.createHorizontalBox();
        knimeBox.add(Box.createHorizontalGlue());
        knimeBox.add(new JLabel("KNIME executable (remote path):"));
        knimeBox.add(m_knimePath);
        m_knimePath.setToolTipText("The path to the knime executable on the Unicore site.");
        //m_knimeBrowseBtn.addActionListener(new FileSelectionListener(
        //        m_knimePath, false));
        //knimeBox.add(m_knimeBrowseBtn);
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(knimeBox);

        // KNIME Args
        Box knimeArgs = Box.createHorizontalBox();
        knimeArgs.add(Box.createHorizontalGlue());
        knimeArgs.add(new JLabel("Command line arguments for KNIME:"));
        knimeArgs.add(m_knimeArgs);
        m_knimeArgs.setToolTipText("Arguments for KNIME (and VM arguments)");
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(knimeArgs);

        // Unicore Args
        Box unicoreArgs = Box.createHorizontalBox();
        unicoreArgs.add(Box.createHorizontalGlue());
        //unicoreArgs.add(new JLabel("Unicore Job Preferences"));
        unicoreArgs.add(new JLabel("Runtime  "));
        unicoreArgs.add(m_unicore_resources_runtime);
        unicoreArgs.add(new JLabel("  Memory  "));
        unicoreArgs.add(m_unicore_resources_memory);
        borderedBox.add(unicoreArgs);
        
        Box unicoreNodeArgs = Box.createHorizontalBox();
        unicoreNodeArgs.add(Box.createHorizontalGlue());
        unicoreNodeArgs.add(new JLabel("Cluster Nodes  "));
        unicoreNodeArgs.add(m_unicore_resources_nodes);
        unicoreNodeArgs.add(new JLabel("  CPUs per Node  "));
        unicoreNodeArgs.add(m_unicore_resources_cpusPerNode);
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(unicoreNodeArgs);
        
        /*// SGE Native Args
        Box nativeArgs = Box.createHorizontalBox();
        nativeArgs.add(Box.createHorizontalGlue());
        nativeArgs.add(new JLabel(
                "Arguments for the SGE job (native arguments):"));
        nativeArgs.add(m_nativeArgs);
        m_nativeArgs.setToolTipText("e.g. qsub arguments go here");
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(nativeArgs);*/

        // Shared Dir (local)
        Box localDir = Box.createHorizontalBox();
        localDir.add(Box.createHorizontalGlue());
        localDir.add(new JLabel("Location of shared directory (local path):"));
        localDir.add(m_localShared);
        m_localShared.setToolTipText("path to a directory accessible from the"
                + " cluster and from this computer");
        m_localBrowseBtn.addActionListener(new FileSelectionListener(
                m_localShared, true));
        localDir.add(m_localBrowseBtn);
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(localDir);

        // Shared Dir (cluster)
        Box clusterDir = Box.createHorizontalBox();
        clusterDir.add(Box.createHorizontalGlue());
        clusterDir.add(new JLabel("Shared directory (remote path):"));
        clusterDir.add(m_clusterShared);
        m_clusterShared.setToolTipText("Optional. Relative to storage sink.");
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(clusterDir);

        // Export client preferences onto grid
        Box useClientPreferences = Box.createHorizontalBox();
        useClientPreferences.add(Box.createHorizontalGlue());
        useClientPreferences.add(m_useClientPreferencesOnGrid);
        m_useClientPreferencesOnGrid
                .setToolTipText("Whether to use local preferences (such as paths to license "
                        + "files) also on the grid machines");
        borderedBox.add(Box.createVerticalStrut(5));
        borderedBox.add(useClientPreferences);

        // Select the shell used in the job script
        Box invokeShellBox = Box.createVerticalBox();
        invokeShellBox
                .setBorder(BorderFactory.createTitledBorder(BorderFactory
                        .createEtchedBorder(),
                        "Select shell used in grid job script:"));
        ButtonGroup group = new ButtonGroup();
        group.add(m_invokeShellBash);
        group.add(m_invokeShellTcsh);
        m_invokeShellBash.doClick(); // select one
        Box bashBox = Box.createHorizontalBox();
        bashBox.add(m_invokeShellBash);
        bashBox.add(Box.createHorizontalGlue());
        Box tcshBox = Box.createHorizontalBox();
        tcshBox.add(m_invokeShellTcsh);
        tcshBox.add(Box.createHorizontalGlue());
        invokeShellBox.add(Box.createHorizontalStrut(15));
        invokeShellBox.add(bashBox);
        invokeShellBox.add(Box.createHorizontalStrut(7));
        invokeShellBox.add(tcshBox);

        // Deletion of temp files after execution
        Box delBox = Box.createVerticalBox();
        delBox.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(),
                "Remove temporary files in shared directory "
                        + "after node execution"));
        group = new ButtonGroup();
        group.add(m_delTempFilesAlways);
        group.add(m_delTempFilesIfSucceeds);
        group.add(m_delTempFilesNever);
        m_delTempFilesAlways.doClick(); // select any one
        Box alwaysBox = Box.createHorizontalBox();
        alwaysBox.add(m_delTempFilesAlways);
        alwaysBox.add(Box.createHorizontalGlue());
        Box successBox = Box.createHorizontalBox();
        successBox.add(m_delTempFilesIfSucceeds);
        successBox.add(Box.createHorizontalGlue());
        Box neverBox = Box.createHorizontalBox();
        neverBox.add(m_delTempFilesNever);
        neverBox.add(Box.createHorizontalGlue());

        delBox.add(alwaysBox);
        delBox.add(Box.createVerticalStrut(5));
        delBox.add(successBox);
        delBox.add(Box.createVerticalStrut(5));
        delBox.add(neverBox);

        // create the panel
        add(Box.createVerticalStrut(5));
        add(prefsBox);
        add(Box.createVerticalStrut(5));
        add(borderedBox);
        add(Box.createVerticalStrut(5));
        add(invokeShellBox);
        add(Box.createVerticalStrut(5));
        //add(delBox);
        //add(Box.createVerticalGlue());

        m_loading.decrementAndGet();

    }

    /**
     * En- or disables all components.
     *
     * @param enable the new enable status to set
     */
    private void setEnabledComponents(final boolean enable) {
        m_setToDefaultVals.setEnabled(enable);
        m_clusterShared.setEnabled(enable);
        m_useClientPreferencesOnGrid.setEnabled(enable);
        m_knimeArgs.setEnabled(enable);
        m_knimePath.setEnabled(enable);
        m_knimeBrowseBtn.setEnabled(enable);
        m_localShared.setEnabled(enable);
        m_localBrowseBtn.setEnabled(enable);
//        m_nativeArgs.setEnabled(enable);
        
        m_unicore_resources_cpusPerNode.setEnabled(enable);
        m_unicore_resources_memory.setEnabled(enable);
        m_unicore_resources_nodes.setEnabled(enable);
        m_unicore_resources_runtime.setEnabled(enable);
        
        m_invokeShellBash.setEnabled(enable);
        m_invokeShellTcsh.setEnabled(enable);
        m_delTempFilesAlways.setEnabled(enable);
        m_delTempFilesIfSucceeds.setEnabled(enable);
        m_delTempFilesNever.setEnabled(enable);
    }

    private void setDefaultValues() {
        m_loading.incrementAndGet();
        assert !m_usePreferences.isSelected();
        if (m_usePreferences.isSelected()) {
            return;
        }

        UnicoreJobManagerSettings s = new UnicoreJobManagerSettings();
        transferComponentsValuesIntoSettings(s);
        UnicorePreferenceInitializer.getSettingsFromPreferences(s);
        s.setUsePreferences(false);
        transferSettingsIntoComponents(s);
        m_loading.decrementAndGet();
    }
    
    private void usePrefsChanged() {
        m_loading.incrementAndGet();
        if (m_usePreferences.isSelected()) {
            // save the values before we replace them with the global prefs
            m_lastTabSettings = new UnicoreJobManagerSettings();
            transferComponentsValuesIntoSettings(m_lastTabSettings);

            UnicoreJobManagerSettings prefs = new UnicoreJobManagerSettings();
            UnicorePreferenceInitializer.getSettingsFromPreferences(prefs);
            prefs.setUsePreferences(true);
            transferSettingsIntoComponents(prefs);

        } else {
            // apply the values last entered by user
            UnicoreJobManagerSettings s = m_lastTabSettings;
            if (s == null) {
                // if there are no old user settings, use default ones
                s = new UnicoreJobManagerSettings();
            }
            s.setUsePreferences(false);
            transferSettingsIntoComponents(s);
        }
        m_loading.decrementAndGet();
    }

    /**
     * Called by the parent to load new settings into the tab.
     *
     * @param settings the new settings to take over
     * @param inSpecs the new input specs (could be null!)
     */
    void loadSettings(final UnicoreJobManagerSettings settings) {

        m_loading.incrementAndGet();
        // this is a bit awkward: usePrefsChanged evaluates the checkbox
        // and transfers the corresponding settings into the components
        m_lastTabSettings = settings;
        m_usePreferences.setSelected(settings.usePreferences());
        // evaluate the "use preferences" checkmark
        usePrefsChanged();
        m_loading.decrementAndGet();
    }

    /**
     * Takes over the new inspecs.
     *
     * @param inSpecs the new input port specs
     */
    void updateInputSpecs(final PortObjectSpec[] inSpecs) {
        // we don't really need specs...
    }

    /**
     * Called by the parent to get current values saved into the settings
     * object.
     *
     * @param settings the object to write the currently entered values into
     */
    void saveSettings(final UnicoreJobManagerSettings settings) {
        transferComponentsValuesIntoSettings(settings);
    }

    /**
     * Transfers the currently entered values from this tab's components into
     * the provided settings object. Does not change any other values in the
     * settings object.
     */
    private void transferComponentsValuesIntoSettings(
            final UnicoreJobManagerSettings settings) {
        settings.setUsePreferences(m_usePreferences.isSelected());
        settings.setRemoteRootDir(m_clusterShared.getText());
        settings.setExportPreferencesToCluster(m_useClientPreferencesOnGrid
                .isSelected());
        settings.setCustomKnimeArguments(ClusterJobSplitSettings
                .splitArgumentString(m_knimeArgs.getText().trim()));
        settings.setRemoteKnimeExecutable(m_knimePath.getText());
        settings.setLocalRootDir(new File(m_localShared.getText()));
        
        settings.setJobCPUsPerNode(m_unicore_resources_cpusPerNode.getText());
        settings.setJobMemory(m_unicore_resources_memory.getText());
        settings.setJobNodes(m_unicore_resources_nodes.getText());
        settings.setJobRuntime(m_unicore_resources_runtime.getText());
        

        if (m_invokeShellBash.isSelected()) {
            settings.setScriptShell(InvokeScriptShell.bash);
        } else {
            assert m_invokeShellTcsh.isSelected() : "No invoke shell selected";
            settings.setScriptShell(InvokeScriptShell.tcsh);
        }

        if (m_delTempFilesAlways.isSelected()) {
            settings.setDeleteTempFilePolicy(DeleteTempFilePolicy.Always);
        } else if (m_delTempFilesIfSucceeds.isSelected()) {
            settings.setDeleteTempFilePolicy(DeleteTempFilePolicy.IfJobSucceeds);
        } else {
            assert m_delTempFilesNever.isSelected() : "No check box selected";
            settings.setDeleteTempFilePolicy(DeleteTempFilePolicy.Never);
        }
    }

    /**
     * Simply reads all values from the settings object and transfers them into
     * the dialog's components.
     *
     * @param settings the settings values to display
     */
    private void transferSettingsIntoComponents(
            final UnicoreJobManagerSettings settings) {

        // button groups only change if enabled
        setEnabledComponents(true);

        m_usePreferences.setSelected(settings.usePreferences());

        m_clusterShared.setText(settings.getRemoteRootDir());
        m_useClientPreferencesOnGrid.setSelected(settings
                .exportPreferencesToCluster());
        m_localShared.setText(settings.getLocalRootDir().getAbsolutePath());

        m_knimePath.setText(settings.getRemoteKnimeExecutable());
        m_knimeArgs.setText(StringsToString(settings.getCustomKnimeArguments()));

       
        m_unicore_resources_cpusPerNode.setText(settings.getJobCPUsPerNode());
        m_unicore_resources_memory.setText(settings.getJobMemory());
        m_unicore_resources_nodes.setText(settings.getJobNodes());
        m_unicore_resources_runtime.setText(settings.getJobRuntime());

        switch (settings.getScriptShell()) {
        case bash:
            m_invokeShellBash.doClick();
            break;
        case tcsh:
            m_invokeShellTcsh.doClick();
            break;
        default:
            m_invokeShellBash.doClick();
        }
        switch (settings.getDeleteTempFilePolicy()) {
        case Always:
            m_delTempFilesAlways.doClick();
            break;
        case IfJobSucceeds:
            m_delTempFilesIfSucceeds.doClick();
            break;
        case Never:
            m_delTempFilesNever.doClick();
            break;
        }

        setEnabledComponents(!settings.usePreferences());

    }

    private String StringsToString(final Collection<String> args) {
        if (args == null || args.size() == 0) {
            return "";
        }
        StringBuilder result = new StringBuilder();
        for (String arg : args) {
            boolean hasSpaces = arg.contains(" ");
            boolean hasQuotes = arg.contains("\"");
            if (!hasQuotes) {
                if (!hasSpaces) {
                    result.append(arg);
                } else {
                    result.append("\"").append(arg).append("\"");
                }
            } else {
                result.append("\"");
                for (int c = 0; c < arg.length(); c++) {
                    char charAt = arg.charAt(c);
                    if (charAt == '"') {
                        result.append("\\\"");
                    } else if (charAt == '\\') {
                        result.append("\\\\");
                    } else {
                        result.append(charAt);
                    }
                }
                result.append("\"");
            }
            result.append(" ");
        }
        return result.toString();
    }

    private class FileSelectionListener implements ActionListener {

        private final JTextField m_textField;

        private final boolean m_dir;

        public FileSelectionListener(final JTextField textField,
                final boolean dir) {
            m_textField = textField;
            m_dir = dir;
        }

        @Override
        public void actionPerformed(final ActionEvent e) {
            JFileChooser chooser = new JFileChooser();
            if (m_dir) {
                chooser.setFileSelectionMode(JFileChooser.DIRECTORIES_ONLY);
            } else {
                chooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
            }
            chooser.setAcceptAllFileFilterUsed(true);
            // set current selection (if any)
            if (m_textField.getText() != null
                    && !m_textField.getText().isEmpty()) {
                File f = null;
                try {
                    f = new File(m_textField.getText());
                } catch (NullPointerException npe) {
                    // do nothing -> file not found
                }
                chooser.setSelectedFile(f);
            }
            int retVal = chooser.showOpenDialog(UnicoreSettingsPrefPanel.this);
            if (retVal == JFileChooser.APPROVE_OPTION) {
                // set text
                File exe = chooser.getSelectedFile();
                m_textField.setText(exe.getAbsolutePath());
            }
        }

    }
}
