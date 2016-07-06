package org.tudresden.unicore.knime;

import org.knime.core.node.NodeLogger;
import org.knime.core.node.workflow.NodeExecutionJobManager;
import org.knime.core.node.workflow.NodeExecutionJobManagerFactory;

/**
 * factory class for job manager
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */

public class UnicoreNodeExecutionJobManagerFactory
implements NodeExecutionJobManagerFactory {

	private static UnicoreNodeExecutionJobManagerFactory instance;

    static final String ID =
        "org.tudresden.unicore.knime.UnicoreNodeExecutionJobManagerFactory";

    /**
     * Don't instantiate this job manager explicitly. The core instantiates all
     * extension of the corresponding extension point and maintains a pool.
     */
    public UnicoreNodeExecutionJobManagerFactory() {

        // lets make sure we have only one instance of this guy.
        if (instance != null) {
            NodeLogger.getLogger(UnicoreNodeExecutionJobManagerFactory.class).error(
                    "Multiple instances of the Unicore Job Manager are created!");
        }
        instance = this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getID() {
        return ID;
    }

    /** {@inheritDoc} */
    @Override
    public String getLabel() {
        return "Unicore Cluster Computation Engine";
    }

    /** {@inheritDoc} */
    @Override
    public NodeExecutionJobManager getInstance() {
        return new UnicoreNodeExecutionJobManager();
    }
	
}
