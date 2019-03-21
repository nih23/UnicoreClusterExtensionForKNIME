package org.tudresden.unicore.knime.executor.settings;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.ItemListener;
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
import javax.swing.JRadioButton;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;

import org.knime.cluster.api.ClusterJobSubSettingsPanel;
import org.knime.cluster.api.ExecutorParametersNodeSettingsPanel;
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

public class UnicoreJobManagerSettingsPanel extends ExecutorParametersNodeSettingsPanel {

	private static final long serialVersionUID = -9110449239215031732L;
	//private final UnicoreSettingsChunkPanel m_chunkPanel;
    //private final UnicoreSettingsPrefPanel m_prefsPanel;
    //private final SplitType m_nodeSplitType;

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

    private final JTextField m_unicore_gateway = new JTextField(15);
    
    private final JTextField m_unicore_site = new JTextField(10);

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
	
    Box prefsBox = Box.createHorizontalBox();
	
    /**
     * @param nodeSplitType type of splitting permitted by the underlying node
     */
    public UnicoreJobManagerSettingsPanel(UnicoreJobManagerSettings settings) {
        super(settings);
        
      setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

      m_loading.incrementAndGet();

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
      Box unicoreGwArgs = Box.createHorizontalBox();
      unicoreGwArgs.add(new JLabel("Gateway  "));
      unicoreGwArgs.add(m_unicore_gateway);
      borderedBox.add(unicoreGwArgs);
      
      Box unicoreGwArgs2 = Box.createHorizontalBox();
      unicoreGwArgs2.add(new JLabel("Queue  "));
      unicoreGwArgs2.add(m_unicore_site);
      borderedBox.add(unicoreGwArgs2);
      
      Box unicoreArgs = Box.createHorizontalBox();
      unicoreArgs.add(Box.createHorizontalGlue());
      //unicoreArgs.add(new JLabel("Unicore Job Preferences"));
      unicoreArgs.add(new JLabel("Runtime  "));
      unicoreArgs.add(m_unicore_resources_runtime);
      unicoreArgs.add(new JLabel("  Memory  "));
      unicoreArgs.add(m_unicore_resources_memory);
      unicoreArgs.add(Box.createHorizontalGlue());
      borderedBox.add(unicoreArgs);
      
      Box unicoreNodeArgs = Box.createHorizontalBox();
      unicoreNodeArgs.add(Box.createHorizontalGlue());
      unicoreNodeArgs.add(new JLabel("Cluster Nodes  "));
      unicoreNodeArgs.add(m_unicore_resources_nodes);
      unicoreNodeArgs.add(new JLabel("  CPUs per Node  "));
      unicoreNodeArgs.add(m_unicore_resources_cpusPerNode);
      borderedBox.add(Box.createVerticalStrut(5));
      borderedBox.add(unicoreNodeArgs);
      
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

//      // create the panel
//      add(Box.createVerticalStrut(5));
//      add(prefsBox);
//      add(Box.createVerticalStrut(5));
//      add(borderedBox);
//      add(Box.createVerticalStrut(5));
//      add(invokeShellBox);
//      add(Box.createVerticalStrut(5));
//      //add(delBox);
//      //add(Box.createVerticalGlue());

      m_loading.decrementAndGet();
    }
    
	@Override
	public String getTabName() {
		return "Unicore Settings";
	}

	@Override
	protected String getParametersName() {
		return "unicore";
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
          int retVal = chooser.showOpenDialog(UnicoreJobManagerSettingsPanel.this);
          if (retVal == JFileChooser.APPROVE_OPTION) {
              // set text
              File exe = chooser.getSelectedFile();
              m_textField.setText(exe.getAbsolutePath());
          }
      }
  }
}
