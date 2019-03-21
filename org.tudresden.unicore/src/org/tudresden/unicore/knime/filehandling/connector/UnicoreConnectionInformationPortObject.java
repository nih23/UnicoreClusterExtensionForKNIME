/**
 * 
 */
package org.tudresden.unicore.knime.filehandling.connector;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.zip.ZipEntry;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformationPortObject;
import org.knime.core.data.util.NonClosableInputStream;
import org.knime.core.data.util.NonClosableOutputStream;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.NodeLogger;
import org.knime.core.node.port.PortObjectSpec;
import org.knime.core.node.port.PortObjectZipInputStream;
import org.knime.core.node.port.PortObjectZipOutputStream;
import org.knime.core.node.port.PortType;
import org.knime.core.node.port.PortTypeRegistry;
import org.tudresden.unicore.knime.filehandling.UnicoreConnectionInformation;

/**
 * port object representation of an unicore connection
 * 
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreConnectionInformationPortObject extends ConnectionInformationPortObject {

    private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreConnectionInformationPortObject.class);
    private UnicoreConnectionInformationPortObjectSpec m_connectionInformationPOS;
    
    /**
     * Type of this port.
     */
    public static final PortType TYPE =
        PortTypeRegistry.getInstance().getPortType(UnicoreConnectionInformationPortObject.class);
    
    /**
     * Type of this optional port.
     * @since 3.0
     */
    public static final PortType TYPE_OPTIONAL =
        PortTypeRegistry.getInstance().getPortType(UnicoreConnectionInformationPortObject.class, true);
    
    /**
     * Should only be used by the framework.
     */
    public UnicoreConnectionInformationPortObject() {
        // Used by framework
    }

    /**
     * Creates a port object with the given connection information.
     *
     * @param connectionInformationPOS The spec wrapping the connection
     *            information.
     */
    public UnicoreConnectionInformationPortObject(final UnicoreConnectionInformationPortObjectSpec connectionInformationPOS) {
        super(connectionInformationPOS);
    	if (connectionInformationPOS == null) {
            throw new NullPointerException("Argument must not be null");
        }
        final ConnectionInformation connInfo = connectionInformationPOS.getConnectionInformation();
        if (connInfo == null) {
            throw new NullPointerException("Connection information must be set (is null)");
        }
        m_connectionInformationPOS = connectionInformationPOS;
    }
	
    public static final class Serializer extends PortObjectSerializer<UnicoreConnectionInformationPortObject> {

		@Override
		public void savePortObject(UnicoreConnectionInformationPortObject portObject, PortObjectZipOutputStream out,
				ExecutionMonitor exec) throws IOException, CanceledExecutionException {
			portObject.save(out);
		}

		@Override
		public UnicoreConnectionInformationPortObject loadPortObject(PortObjectZipInputStream in, PortObjectSpec spec, ExecutionMonitor exec) throws IOException, CanceledExecutionException {
			return load(in, (UnicoreConnectionInformationPortObjectSpec) spec);
		} }
    
    
    private void save(final PortObjectZipOutputStream out) {
        ObjectOutputStream oo = null;
        // save weka clusterer
        try {
            out.putNextEntry(new ZipEntry("clusterer.objectout"));
            oo = new ObjectOutputStream(new NonClosableOutputStream.Zip(out));
            oo.writeObject(m_connectionInformationPOS.getConnectionInformation());
        } catch (IOException ioe) {
            LOGGER.error("Internal error: Could not save settings", ioe);
        } finally {
            if (oo != null) {
                try {
                    oo.close();
                } catch (Exception e) {
                    LOGGER.debug("Could not close stream", e);
                }
            }
        }
    }

    private static UnicoreConnectionInformationPortObject load(final PortObjectZipInputStream in, final UnicoreConnectionInformationPortObjectSpec spec) {
        ObjectInputStream oi = null;
        UnicoreConnectionInformation loadedSpecWithConnInfo = null;
        try {
            ZipEntry zentry = in.getNextEntry();
            assert zentry.getName().equals("clusterer.objectout");
            oi = new ObjectInputStream(new NonClosableInputStream.Zip(in));
            loadedSpecWithConnInfo = (UnicoreConnectionInformation)oi.readObject();
        } catch (IOException ioe) {
            LOGGER.error("Internal error: Could not load settings", ioe);
        } catch (ClassNotFoundException cnf) {
            LOGGER.error("Internal error: Could not load settings", cnf);
        } finally {
            if (oi != null) {
                try {
                    oi.close();
                } catch (Exception e) {
                    LOGGER.debug("Could not close stream", e);
                }
            }
        }

        assert (loadedSpecWithConnInfo != null);      
        
        return new UnicoreConnectionInformationPortObject(new UnicoreConnectionInformationPortObjectSpec(loadedSpecWithConnInfo));
    }
    
    /**
     * Returns the connection information contained by this port object.
     *
     *
     * @return The content of this port object
     */
    public UnicoreConnectionInformation getConnectionInformation() {
        return m_connectionInformationPOS.getConnectionInformation();
    }
    
    /**
     * {@inheritDoc}
     */
    @Override
    public PortObjectSpec getSpec() {
        return m_connectionInformationPOS;
    }
    
}
