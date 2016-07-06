package org.tudresden.unicore.knime;

import org.knime.cluster.executor.AbstractChunkedClusterJob;
import org.knime.cluster.executor.AbstractClusterJobManager;
import org.knime.cluster.executor.settings.ClusterJobSplitSettings;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.port.PortObject;
import org.knime.core.node.workflow.NodeContainer;
import org.knime.core.node.workflow.NodeExecutionJobReconnectException;

/**
*
* @author (mostly) ohl, KNIME.com, Switzerland
*/
public class ChunkedUnicoreJob extends AbstractChunkedClusterJob {
	/**
     * Creates a new Unicore split job.
     *
     * @param jobMgr the corresponding job manager
     * @param nc the node
     * @param data the input objects to the node
     * @param settings the job manager settings for the execution
     */
    public ChunkedUnicoreJob(final AbstractClusterJobManager jobMgr,
            final NodeContainer nc, final PortObject[] data,
            final ClusterJobSplitSettings settings) {
        super(jobMgr, nc, data, settings);
    }

    /**
     * Creates a reconnecting split job.
     *
     * @param jobMgr the corresponding job manager
     * @param reconnectInfo info stored before the job was disconnected
     * @param nc the node
     * @param settings the job manager settings for the execution
     *
     * @throws InvalidSettingsException if reconnect info is invalid
     * @throws NodeExecutionJobReconnectException if something went wrong
     */
    public ChunkedUnicoreJob(final AbstractClusterJobManager jobMgr,
            final NodeSettingsRO reconnectInfo, final PortObject[] inData,
            final NodeContainer nc, final ClusterJobSplitSettings settings)
            throws InvalidSettingsException, NodeExecutionJobReconnectException {
        super(jobMgr, reconnectInfo, inData, nc, settings);
    }
}
