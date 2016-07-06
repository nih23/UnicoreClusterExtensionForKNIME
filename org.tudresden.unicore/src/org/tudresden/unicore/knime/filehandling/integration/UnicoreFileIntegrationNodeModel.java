package org.tudresden.unicore.knime.filehandling.integration;

import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.knime.base.filehandling.remote.files.Connection;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileFactory;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataColumnSpec;
import org.knime.core.data.DataColumnSpecCreator;
import org.knime.core.data.DataRow;
import org.knime.core.data.DataTableSpec;
import org.knime.core.data.container.ColumnRearranger;
import org.knime.core.data.container.SingleCellFactory;
import org.knime.core.data.uri.URIDataValue;
import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionContext;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortType;
import org.tudresden.unicore.knime.filehandling.UnicoreConnectionInformation;
import org.tudresden.unicore.knime.filehandling.UnicoreRemoteFile;
import org.tudresden.unicore.knime.filehandling.connector.UnicoreConnectionInformationPortObject;
import org.tudresden.unicore.knime.filehandling.connector.UnicoreConnectionInformationPortObjectSpec;

/**
 * This is the model implementation.
 *
 *
 * @author Nico Hoffmann, Patrick Winter
 */
public class UnicoreFileIntegrationNodeModel extends NodeModel {

	public final static String u6PathColumnName = "URI";
	public final static String resolvedPathColumnName = "U6";
	final static String[] unicoreUseModes = new String[] {"Local Data Integration", "Remote Execution"  };
	static int unicoreRemoteExecutionUseModeIndex = 1;

	static final String PropsUseMode = "usemode";
	static final String PropsLocalDirectory = "localDirectory";
	static final String PropsS3Staging = "s3staging";

	private final SettingsModelString m_useMode = new SettingsModelString(PropsUseMode, null);

	private final SettingsModelString m_localDirectory =
			new SettingsModelString(PropsLocalDirectory, null);

	private final SettingsModelBoolean m_s3staging =
			new SettingsModelBoolean(PropsS3Staging, false);

	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreFileIntegrationNodeModel.class);

	private UnicoreConnectionInformation m_connectionInformation;

	private boolean m_abort;


	/**
	 * Constructor for the node model.
	 */
	public UnicoreFileIntegrationNodeModel() {
		super(new PortType[]{UnicoreConnectionInformationPortObject.TYPE, BufferedDataTable.TYPE}, new PortType[]{BufferedDataTable.TYPE});
	}

	@Override
	protected PortObject[] execute(final PortObject[] inObjects, final ExecutionContext exec) throws Exception {
		// Create connection monitor
		final ConnectionMonitor<Connection> monitor = new ConnectionMonitor<>();

		//retrieve data from in data table and create local path => target  	
		// Get table with source URIs
		final BufferedDataTable inTable = (BufferedDataTable)inObjects[1];
		final int sourceIndex = inTable.getDataTableSpec().findColumnIndex(UnicoreFileIntegrationNodeModel.u6PathColumnName);

		// start URI conversion
		ColumnRearranger rearranger = createColumnRearranger(inTable.getDataTableSpec(), sourceIndex, exec, monitor);
		BufferedDataTable outTable = exec.createColumnRearrangeTable(
				inTable, rearranger, exec);
		return new BufferedDataTable[]{outTable};
	}

	/**
	 * process input file names to either create remote or local links
	 * @param spec
	 * @param sourceIndex
	 * @param exec
	 * @param monitor
	 * @return
	 * @throws InvalidSettingsException
	 */
	private ColumnRearranger createColumnRearranger(DataTableSpec spec, final int sourceIndex, final ExecutionContext exec, final ConnectionMonitor<Connection> monitor) throws InvalidSettingsException {
		ColumnRearranger result = new ColumnRearranger(spec);
		DataColumnSpecCreator appendSpecCreator = new DataColumnSpecCreator(UnicoreFileIntegrationNodeModel.resolvedPathColumnName, UnicoreRemoteFileDataCell.TYPE);
		DataColumnSpec appendSpec = appendSpecCreator.createSpec();
		
		result.append(new SingleCellFactory(true, appendSpec) {

			@Override
			public DataCell getCell(DataRow row) {				
				// Get source URI from table cell		
				final URI sourceUri = ((URIDataValue)row.getCell(sourceIndex)).getURIContent().getURI();
				boolean dataStaging = m_s3staging.getBooleanValue();
				RemoteFile sourceFile = createRemoteFile(sourceUri, monitor);
				UnicoreRemoteFile unicoreSourceFile = (UnicoreRemoteFile)sourceFile;
				unicoreSourceFile.integrateViaStaging(m_s3staging.getBooleanValue()); //TODO: NPE
				return new UnicoreRemoteFileDataCell(unicoreSourceFile);
			}
		});
		return result;
	}

	private RemoteFile<Connection> createRemoteFile(URI sourceUri, ConnectionMonitor<Connection> monitor) {
		if(! sourceUri.getScheme().equals("u6")) {
			LOGGER.error("Unicore File Integration node only supports unicore files in input table");
			//throw new InvalidSettingsExceSption("Unicore File Integration node only supports unicore files in input table");
		}
		UnicoreConnectionInformation connectionInformation;

		// validate URI on input file
		try {
			m_connectionInformation.fitsToURI(sourceUri);
			connectionInformation = m_connectionInformation;
		} catch (final Exception e) {
			connectionInformation = null;
			LOGGER.error("Unicore File Integration node only supports unicore files in input table", e);
			return null;
		}

		// Create source file
		RemoteFile<Connection> sourceFile;
		try {
			sourceFile = RemoteFileFactory.createRemoteFile(sourceUri, connectionInformation, monitor);
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
			return null;
		}

		return sourceFile;
	}

	/**
	 * create out table spec by adding URIOnTarget column to the in-table's columns 
	 * @param inSpecs
	 * @return
	 */
	private DataTableSpec createOutSpec(DataTableSpec inSpecs) {

		int n = inSpecs.getNumColumns();
		DataColumnSpec[] columnSpecs = new DataColumnSpec[n+1];
		for(int i = 0; i < n ; i++) {
			columnSpecs[i] = inSpecs.getColumnSpec(i);
		}
		columnSpecs[n] = new DataColumnSpecCreator(UnicoreFileIntegrationNodeModel.resolvedPathColumnName, UnicoreRemoteFileDataCell.TYPE).createSpec();

		return new DataTableSpec(columnSpecs);
	}

	/**
	 * Check if the file fits the filter.
	 *
	 *
	 * @param name Name of the file
	 * @return true if it fits, false if it gets filtered out
	 */
	private boolean fitsFilter(final String name) {
		return true;
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected PortObjectSpec[] configure(final PortObjectSpec[] inSpecs) throws InvalidSettingsException {
		// Check if a port object is available
		if (inSpecs[0] == null) {
			throw new InvalidSettingsException("No connection information available");
		}

		if (inSpecs[1] == null) {
			throw new InvalidSettingsException("No file table available");
		}

		final UnicoreConnectionInformationPortObjectSpec object = (UnicoreConnectionInformationPortObjectSpec)inSpecs[0];
		m_connectionInformation = object.getConnectionInformation();
		// Check if the port object has connection information
		if (m_connectionInformation == null) {
			throw new InvalidSettingsException("No connection information available");
		}	
		Object tableSpecs = inSpecs[1];
		if(!(tableSpecs instanceof DataTableSpec)) {
			throw new InvalidSettingsException("Expected file table as second input");
		}
		DataTableSpec inTblSpec = (DataTableSpec)tableSpecs;
		return new PortObjectSpec[]{createOutSpec(inTblSpec)};
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
	CanceledExecutionException {
		// not used
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveInternals(final File nodeInternDir, final ExecutionMonitor exec) throws IOException,
	CanceledExecutionException {
		// not used
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void saveSettingsTo(final NodeSettingsWO settings) {
		m_localDirectory.saveSettingsTo(settings);
		m_s3staging.saveSettingsTo(settings);
		m_useMode.saveSettingsTo(settings);	
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void validateSettings(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_localDirectory.validateSettings(settings);
		m_s3staging.validateSettings(settings);
		m_useMode.validateSettings(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void loadValidatedSettingsFrom(final NodeSettingsRO settings) throws InvalidSettingsException {
		m_localDirectory.loadSettingsFrom(settings);
		m_s3staging.loadSettingsFrom(settings);
		m_useMode.loadSettingsFrom(settings);
	}

	/**
	 * {@inheritDoc}
	 */
	@Override
	protected void reset() {
		// not used
	}

}
