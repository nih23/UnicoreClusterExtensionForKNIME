package org.tudresden.unicore.knime.executor.settings;

import java.awt.Dimension;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.text.ParseException;
import java.util.Vector;

import javax.swing.BorderFactory;
import javax.swing.Box;
import javax.swing.BoxLayout;
import javax.swing.ButtonGroup;
import javax.swing.DefaultComboBoxModel;
import javax.swing.JCheckBox;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.JSpinner;
import javax.swing.JSpinner.DefaultEditor;
import javax.swing.SpinnerNumberModel;

import org.knime.core.data.DataTableSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.workflow.NodeContainer.NodeContainerSettings.SplitType;

/**
 * settings panel for (wrapped)metanodes to specify input splitting
 *
 * @author ohl, University of Konstanz
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreSettingsChunkPanel extends JPanel {

    private final JCheckBox m_chunkExec = new JCheckBox();
    private final JComboBox m_portIndex = new JComboBox();
    private final JRadioButton m_useChunkCount = new JRadioButton();
    private final JRadioButton m_useChunkSize = new JRadioButton();
    private final JSpinner m_chunkCount = new JSpinner();
    private final JSpinner m_chunkSize = new JSpinner();
    private final JCheckBox m_alwaysUniquify = new JCheckBox();
    private final SplitType m_nodeSplitType;

    /**
     * Creates a new tab.
     *
     * @param nodeSplitType splitting permitted by underlying node
     */
    public UnicoreSettingsChunkPanel(final SplitType nodeSplitType) {
        setLayout(new BoxLayout(this, BoxLayout.Y_AXIS));

        m_nodeSplitType = nodeSplitType;

        // chunked execution
        Box chunkBox = Box.createVerticalBox();
        chunkBox.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Settings for split execution"));

        // enable chunks
        Box enableChunkBox = Box.createHorizontalBox();
        m_chunkExec.setText("Split input table and execute node on chunks");
        m_chunkExec.setToolTipText("Enable this only if you are certain that"
                + " the node does not need access to the entire input table");
        m_chunkExec.addItemListener(new ItemListener() {
            public void itemStateChanged(final ItemEvent e) {
                chunkExecSelectionChanged();
            }
        });
        m_chunkExec.setSelected(false);
        m_chunkExec.setEnabled(!SplitType.DISALLOWED.equals(m_nodeSplitType));
        enableChunkBox.add(Box.createHorizontalGlue());
        enableChunkBox.add(m_chunkExec);
        enableChunkBox.add(Box.createHorizontalGlue());

        // select input port to split
        Box portNumberBox = Box.createHorizontalBox();
        portNumberBox.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Split port selection"));
        Box leftPortBox = Box.createHorizontalBox();
        leftPortBox
                .add(new JLabel("Split data table on input port with index:"));
        leftPortBox.add(Box.createHorizontalStrut(31));
        leftPortBox.add(Box.createHorizontalGlue());
        m_portIndex.setToolTipText("Select the data port whose "
                + "table is split into chunks");
        m_portIndex.setMaximumSize(new Dimension(100, 25));
        m_portIndex.setPreferredSize(new Dimension(100, 25));
        leftPortBox.add(m_portIndex);
        portNumberBox.add(Box.createHorizontalStrut(9));
        portNumberBox.add(leftPortBox);
        portNumberBox.add(Box.createHorizontalGlue());
        portNumberBox.add(Box.createHorizontalGlue());

        // select number of chunks
        Box chunkCount = Box.createHorizontalBox();
        chunkCount.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Specify number of split jobs"));
        m_chunkCount
                .setModel(new SpinnerNumberModel(5, 1, Integer.MAX_VALUE, 1));
        m_chunkCount.setToolTipText("This many grid jobs will be created.");
        m_chunkCount.setMaximumSize(new Dimension(100, 25));
        m_chunkCount.setPreferredSize(new Dimension(100, 25));
        m_chunkSize
                .setModel(new SpinnerNumberModel(20, 1, Integer.MAX_VALUE, 1));
        m_chunkSize.setToolTipText("Size of input table to each grid job"
                + "(will be adjusted to have equally sized input tables).");
        m_chunkSize.setMaximumSize(new Dimension(100, 25));
        m_chunkSize.setPreferredSize(new Dimension(100, 25));
        m_useChunkCount.setText("fixed number of jobs:");
        m_useChunkSize.setText("fixed size of input table per job:");
        ButtonGroup bg = new ButtonGroup();
        bg.add(m_useChunkCount);
        bg.add(m_useChunkSize);
        bg.setSelected(m_useChunkCount.getModel(), true);
        m_useChunkCount.addItemListener(new ItemListener() {
            public void itemStateChanged(final ItemEvent e) {
                if (e.getStateChange() == ItemEvent.SELECTED) {
                    chunkSizeButtonGroupChanged();
                }
            }
        });
        m_useChunkSize.addItemListener(new ItemListener() {
            public void itemStateChanged(final ItemEvent e) {
                if (e.getStateChange() == ItemEvent.SELECTED) {
                    chunkSizeButtonGroupChanged();
                }
            }
        });
        Box chCntBox = Box.createHorizontalBox();
        chCntBox.add(m_useChunkCount);
        chCntBox.add(Box.createHorizontalStrut(7));
        chCntBox.add(Box.createHorizontalGlue());
        chCntBox.add(m_chunkCount);
        Box chSizeBox = Box.createHorizontalBox();
        chSizeBox.add(m_useChunkSize);
        chSizeBox.add(Box.createHorizontalStrut(7));
        chSizeBox.add(Box.createHorizontalGlue());
        chSizeBox.add(new JLabel("no. rows:"));
        chSizeBox.add(Box.createHorizontalStrut(3));
        chSizeBox.add(m_chunkSize);
        Box leftBox = Box.createVerticalBox();
        leftBox.add(chCntBox);
        leftBox.add(chSizeBox);
        chunkCount.add(Box.createHorizontalStrut(5));
        chunkCount.add(leftBox);
        chunkCount.add(Box.createHorizontalGlue());
        chunkCount.add(Box.createHorizontalGlue());

        // always uniquify checkbox
        Box uniBox = Box.createHorizontalBox();
        uniBox.setBorder(BorderFactory.createTitledBorder(BorderFactory
                .createEtchedBorder(), "Row ID uniquification"));
        m_alwaysUniquify.setText("append index to result row IDs");
        m_alwaysUniquify
                .setToolTipText("if not checked, only a quick check is "
                        + "performed to find out if uniquification is needed");
        uniBox.add(Box.createHorizontalStrut(5));
        uniBox.add(m_alwaysUniquify);
        uniBox.add(Box.createHorizontalGlue());

        // put it in the chunk settings box
        chunkBox.add(enableChunkBox);
        chunkBox.add(Box.createVerticalStrut(5));
        chunkBox.add(portNumberBox);
        chunkBox.add(Box.createVerticalStrut(5));
        chunkBox.add(chunkCount);
        chunkBox.add(Box.createVerticalStrut(5));
        chunkBox.add(uniBox);
        chunkBox.add(Box.createVerticalGlue());
        chunkBox.add(Box.createVerticalGlue());

        // create the panel
        add(chunkBox);
        add(Box.createVerticalGlue());

        // propagate the enable status to the components
        chunkSizeButtonGroupChanged();
        chunkExecSelectionChanged();
    }

    /**
     * Called by the parent to load new settings into the tab.
     *
     * @param settings the new settings to take over
     * @param inSpecs the new input specs (could be null!)
     */
    void loadSettings(final UnicoreJobManagerSettings settings) {
        transferSettingsIntoComponents(settings);
    }

    /**
     * Takes over the new input port specs.
     *
     * @param inSpecs the new input specs
     */
    void updateInputSpecs(final PortObjectSpec[] inSpecs) {

        Integer selPortIdx = (Integer)m_portIndex.getSelectedItem();

        // set all data ports as selectable split ports
        Vector<Integer> dataPortIdx = new Vector<Integer>();
        for (int i = 0; i < inSpecs.length; i++) {
            if (inSpecs[i] instanceof DataTableSpec) {
                dataPortIdx.add(i);
            }
        }
        m_portIndex.setModel(new DefaultComboBoxModel(dataPortIdx));
        m_portIndex.setSelectedItem(selPortIdx);
        if (dataPortIdx.size() > 0) {
            if (selPortIdx == null
                    || m_portIndex.getSelectedItem() != selPortIdx) {
                m_portIndex.setSelectedIndex(0);
            }
        }
    }

    /**
     * Called by the parent to get current values saved into the settings
     * object.
     *
     * @param settings the object to write the currently entered values into
     * @throws InvalidSettingsException if values can't be saved into settings
     *             object
     */
    void saveSettings(final UnicoreJobManagerSettings settings)
            throws InvalidSettingsException {
        transferComponentsValuesIntoSettings(settings);
    }

    private void chunkExecSelectionChanged() {
        setEnabledOnChunkComponents(m_chunkExec.isSelected());
    }

    private void chunkSizeButtonGroupChanged() {
        setEnabledOnChunkComponents(true);
    }

    /**
     * En- or disables all components related to chunked execution.
     *
     * @param enable the new enable status to set.
     */
    private void setEnabledOnChunkComponents(final boolean enable) {
        m_portIndex.setEnabled(enable);
        m_alwaysUniquify.setEnabled(enable);
        m_useChunkCount.setEnabled(enable);
        m_useChunkSize.setEnabled(enable);
        if (!enable) {
            m_chunkCount.setEnabled(false);
            m_chunkSize.setEnabled(false);
        } else {
            m_chunkCount.setEnabled(m_useChunkCount.isSelected());
            m_chunkSize.setEnabled(m_useChunkSize.isSelected());
        }
    }

    /**
     * Transfers the values currently entered in the tab's components into the
     * provided settings object. Modifies only the values which correspond with
     * components of this tab.
     */
    private void transferComponentsValuesIntoSettings(
            final UnicoreJobManagerSettings result) throws InvalidSettingsException {

        // commit pending changes in the spinner
        try {
            m_chunkCount.commitEdit();
        } catch (ParseException e) {
            // reset the value also in the GUI
            JComponent editor = m_chunkCount.getEditor();
            if (editor instanceof DefaultEditor) {
                ((DefaultEditor)editor).getTextField().setValue(
                        m_chunkCount.getValue());
            }
            if (m_chunkCount.isEnabled()) {
                throw new InvalidSettingsException(
                        "Invalid number of partitions."
                                + " Value reset. Please verify.");
            }
        }
        try {
            m_chunkSize.commitEdit();
        } catch (ParseException e) {
            // reset the value also in the GUI
            JComponent editor = m_chunkSize.getEditor();
            if (editor instanceof DefaultEditor) {
                ((DefaultEditor)editor).getTextField().setValue(
                        m_chunkSize.getValue());
            }
            if (m_chunkSize.isEnabled()) {
                throw new InvalidSettingsException(
                        "Invalid number of partitions."
                                + " Value reset. Please verify.", e);
            }
        }
        // split port index
        Integer idx = (Integer)m_portIndex.getSelectedItem();
        if (idx != null) {
            result.setSplitPortIdx(idx);
        } else {
            if (m_chunkExec.isSelected()) {
                throw new InvalidSettingsException(
                        "Select the port whose table should be split");
            }
            result.setSplitPortIdx(0);
        }

        // now transfer the values in the result
        result.setUseChunkSize(m_useChunkSize.isSelected());
        result.setNumberOfChunks((Integer)(((SpinnerNumberModel)m_chunkCount
                .getModel()).getValue()));
        result.setNumOfRowsPerChunk((Integer)(((SpinnerNumberModel)m_chunkSize
                .getModel()).getValue()));
        result.setSplitExecution(m_chunkExec.isSelected());
        result.setAlwaysUniquifyIDs(m_alwaysUniquify.isSelected());

    }

    /**
     * Simply reads all values from the settings object and transfers them into
     * the dialog.
     *
     * @param settings the settings values to display
     */
    private void transferSettingsIntoComponents(
            final UnicoreJobManagerSettings settings) {

        int numOfChunks = settings.getNumberOfChunks();
        if (numOfChunks <= 0) {
            numOfChunks = 5;
        }
        ((SpinnerNumberModel)m_chunkCount.getModel()).setValue(numOfChunks);

        int chunkSize = settings.getNumOfRowsPerChunk();
        if (chunkSize <= 0) {
            chunkSize = 20;
        }
        ((SpinnerNumberModel)m_chunkSize.getModel()).setValue(chunkSize);

        m_useChunkSize.setSelected(settings.useChunkSize());
        m_useChunkCount.setSelected(!settings.useChunkSize());

        m_portIndex.setSelectedItem(settings.getSplitPortIdx());
        m_alwaysUniquify.setSelected(settings.alwaysUniquifyIDs());

        if (SplitType.DISALLOWED.equals(m_nodeSplitType)) {
            m_chunkExec.setSelected(false);
            m_chunkExec.setEnabled(false);
        } else {
            m_chunkExec.setEnabled(true);
            m_chunkExec.setSelected(settings.splitExecution());
        }
        chunkExecSelectionChanged(); // set enable status accordingly

    }
}

