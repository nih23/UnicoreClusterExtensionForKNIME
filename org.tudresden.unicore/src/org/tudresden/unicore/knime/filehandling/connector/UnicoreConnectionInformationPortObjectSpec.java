/**
 * 
 */
package org.tudresden.unicore.knime.filehandling.connector;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObjectSpec;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.ModelContentRO;
import org.knime.core.node.ModelContentWO;
import org.tudresden.unicore.knime.filehandling.UnicoreConnectionInformation;

/**
 * specification of unicore connection information port objects
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreConnectionInformationPortObjectSpec extends ConnectionInformationPortObjectSpec {

    /**
     * @noreference This class is not intended to be referenced by clients.
     * @since 3.0
     */
    public static final class Serializer extends AbstractSimplePortObjectSpecSerializer<UnicoreConnectionInformationPortObjectSpec> { }
	
    private UnicoreConnectionInformation m_connectionInformation;

    public UnicoreConnectionInformationPortObjectSpec() {
    	super();
    }
    
    /**
     * Create specs that contain connection information.
     *
     *
     * @param connectionInformation The content of this port object
     */
    public UnicoreConnectionInformationPortObjectSpec(final UnicoreConnectionInformation connectionInformation) {
        //super(connectionInformation);
    	if (connectionInformation == null) {
            throw new NullPointerException("List argument must not be null");
        }
        m_connectionInformation = connectionInformation;
    }

    /**
     * Return the connection information contained by this port object spec.
     *
     *
     * @return The content of this port object
     */
    public UnicoreConnectionInformation getConnectionInformation() {
        return m_connectionInformation;
    }
	
    /**
     * {@inheritDoc}
     */
    @Override
    protected void save(final ModelContentWO model) {
        m_connectionInformation.save(model);
    }

	@Override
	protected void load(ModelContentRO model) throws InvalidSettingsException {
		// TODO Auto-generated method stub
		
	}

}
