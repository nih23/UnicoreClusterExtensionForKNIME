package org.tudresden.unicore.knime.filehandling.integration;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.base.filehandling.remote.files.RemoteFileHandlerRegistry;
import org.knime.core.data.DataCell;
import org.knime.core.data.DataCellDataInput;
import org.knime.core.data.DataCellDataOutput;
import org.knime.core.data.DataCellSerializer;
import org.knime.core.data.DataType;
import org.knime.core.data.DataTypeRegistry;
import org.knime.core.data.StringValue;
import org.knime.core.data.blob.BinaryObjectDataCell;
import org.knime.core.node.NodeLogger;
import org.tudresden.unicore.knime.filehandling.UnicoreConnection;
import org.tudresden.unicore.knime.filehandling.UnicoreConnectionInformation;
import org.tudresden.unicore.knime.filehandling.UnicoreRemoteFile;
import org.tudresden.unicore.knime.filehandling.UnicoreRemoteFileHandler;

public class UnicoreRemoteFileDataCell extends DataCell implements StringValue {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreRemoteFileDataCell.class);

	public static final DataType TYPE = DataType.getType(UnicoreRemoteFileDataCell.class);
	UnicoreRemoteFile m_urf;
	
	/**
     * Serializer as required by {@link DataCell} class.
     *
     * @return A serializer.
     * @noreference This method is not intended to be referenced by clients.
     * @deprecated use {@link DataTypeRegistry#getSerializer(Class)} instead
     */
    @Deprecated
    public static final DataCellSerializer<UnicoreRemoteFileDataCell> getCellSerializer() {
        return new UnicoreRemoteFileDataCellSerializer();
    }
	
	/**
     * Serializer for {@link BinaryObjectDataCell}s.
     *
     * @noreference This class is not intended to be referenced by clients.
     * @since 3.0
     */
    public static final class UnicoreRemoteFileDataCellSerializer implements DataCellSerializer<UnicoreRemoteFileDataCell> {
            /** {@inheritDoc} */
            @Override
            public UnicoreRemoteFileDataCell deserialize(final DataCellDataInput input)
                    throws IOException {
            	int nBytes = input.readInt();
            	boolean integrateViaStaging = input.readBoolean();
            	byte[] data_uci = new byte[nBytes];
                input.readFully(data_uci);
            	String[] deserConnInfo = new String(data_uci, StandardCharsets.UTF_8).split(";");
            	
            	String gw = deserConnInfo[0];
            	String queue = deserConnInfo[1];
            	String user = deserConnInfo[2];
            	String pass = deserConnInfo[3];
            	String defPath = deserConnInfo[4];
            	String host = deserConnInfo[5];
            	String strgSink = deserConnInfo[6];
            	String protocol = deserConnInfo[7];
            	String port = deserConnInfo[8];
            	String uri = deserConnInfo[9];
            	


            	UnicoreConnectionInformation urf_uci = new UnicoreConnectionInformation();
            	urf_uci.setGateway(gw);
            	urf_uci.setQueue(queue);
            	urf_uci.setUser(user);
            	urf_uci.setPassword(pass);
            	urf_uci.setDefaultPath(defPath);
            	urf_uci.setHost(host);
            	urf_uci.setProtocol(protocol);
            	urf_uci.setStorageSink(strgSink);
            	urf_uci.setPort(Integer.parseInt(port));
            	
            	            	
            	
            	/*boolean integrateViaStaging = input.readBoolean();
            	//TODO: IMPROVE ME!
            	int n_uci = input.readInt();
            	byte[] data_uci = new byte[n_uci];
                input.readFully(data_uci);

                int n_uri = input.readInt();
            	byte[] data_uri = new byte[n_uri];
                input.readFully(data_uri);
               
                ObjectInputStream o_uci = new ObjectInputStream( new ByteArrayInputStream(data_uci) ) ;
                ObjectInputStream o_uri = new ObjectInputStream( new ByteArrayInputStream(data_uri) ) ;
                UnicoreConnectionInformation urf_uci = null;
                java.net.URI urf_uri = null;
				try {
					urf_uci = (UnicoreConnectionInformation) o_uci.readObject();
					urf_uri = (java.net.URI) o_uri.readObject();
				} catch (ClassNotFoundException e) {
					LOGGER.warn(e.getMessage(), e);
				} finally {
					o_uci.close();
					o_uri.close();
				}*/

				UnicoreRemoteFile urf;
				try {
	            	java.net.URI urf_uri = new java.net.URI(uri);
	            	final RemoteFileHandler<UnicoreConnection> handler =
	                        (RemoteFileHandler<UnicoreConnection>)RemoteFileHandlerRegistry.getRemoteFileHandler(urf_uri.getScheme());
					//urf = RemoteFileFactory.createRemoteFile(urf_uri, urf_uci, new ConnectionMonitor<UnicoreConnection>());
	            	urf = (UnicoreRemoteFile) handler.createRemoteFile(urf_uri, urf_uci, new ConnectionMonitor<UnicoreConnection>());
					urf.integrateViaStaging(integrateViaStaging);
					UnicoreRemoteFileDataCell urfdc = new UnicoreRemoteFileDataCell(urf);
					
					// We prolly have to copy more attributes ..
					
					return urfdc;
				} catch (Exception e) {
					LOGGER.warn(e.getMessage(), e);
					
				}
				return null;
            }

        /** {@inheritDoc} */
        @Override
        public void serialize(final UnicoreRemoteFileDataCell cell, final DataCellDataOutput output)
                throws IOException {
        	//TODO: IMPROVE ME!
        	
        	UnicoreRemoteFile urf = cell.getValue();  
        	java.net.URI urf_uri = urf.getURI();
        	UnicoreConnectionInformation uci = (UnicoreConnectionInformation) urf.getConnectionInformation();
        	       	
        	String strToSer = uci.getGateway();
        	strToSer += ";" + uci.getQueue();
        	strToSer += ";" + uci.getUser();
        	strToSer += ";" + uci.getPassword();
        	strToSer += ";" + uci.getDefaultPath();
        	strToSer += ";" + uci.getHost();
        	strToSer += ";" + uci.getStorageSink();
        	strToSer += ";" + uci.getProtocol();
        	strToSer += ";" + uci.getPort();
        	strToSer += ";" + urf_uri.toString();
        	
        	/*UnicoreConnectionInformation uci2 = new UnicoreConnectionInformation();
        	uci2.set
        	
        	ByteArrayOutputStream baos = new ByteArrayOutputStream();
        	ObjectOutputStream    oos  = new ObjectOutputStream( baos );
        	oos.writeObject( uci );
        	oos.close();
        	byte[] data_uci = baos.toByteArray();
        	
            output.writeBoolean(urf.isIntegratedViaStaging());

        	
        	output.writeInt(data_uci.length);
            output.write(data_uci);
            
            ByteArrayOutputStream baos2 = new ByteArrayOutputStream();
        	ObjectOutputStream    oos2  = new ObjectOutputStream( baos2 );
            
            oos2.writeObject(urf_uri);
            oos2.close();
            byte[] data_uri = baos2.toByteArray();
        	output.writeInt(data_uri.length);
            output.write(data_uri);*/
            
            byte[] b = strToSer.getBytes(StandardCharsets.UTF_8); // Java 7+ only
            output.writeInt(b.length);
            output.writeBoolean(urf.isIntegratedViaStaging());
            output.write(b);
        }
    }
	
	private static final long serialVersionUID = 4558742256461000529L;

	public UnicoreRemoteFileDataCell(UnicoreRemoteFile urf) {
		m_urf = urf;
	}
	
	@Override
	public String toString() {
		return getStringValue();
	}

	@Override
	protected boolean equalsDataCell(DataCell dc) {
		if(!(dc instanceof UnicoreRemoteFileDataCell))
			return false;
		UnicoreRemoteFileDataCell urfdc = (UnicoreRemoteFileDataCell)dc;
		return urfdc.getValue().equals(m_urf);
	}
	
	public UnicoreRemoteFile getValue() {
		return m_urf;
	}

	@Override
	public int hashCode() {
		return m_urf.hashCode();
	}

	@Override
	public String getStringValue() {
		String rlp = "";
		try {
			rlp = m_urf.getRemoteLocalPath();
		} catch (Exception e) {
			LOGGER.warn(e.getMessage(), e);
		}
		return "file://" + rlp;
	}

}
