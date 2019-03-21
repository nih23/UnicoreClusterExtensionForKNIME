package org.tudresden.unicore.knime.filehandling;

import java.net.URI;

import org.knime.base.filehandling.remote.connectioninformation.port.ConnectionInformation;
import org.knime.base.filehandling.remote.files.ConnectionMonitor;
import org.knime.base.filehandling.remote.files.Protocol;
import org.knime.base.filehandling.remote.files.RemoteFile;
import org.knime.base.filehandling.remote.files.RemoteFileHandler;
import org.knime.core.node.NodeLogger;
import org.tudresden.unicore.knime.UnicoreExecutorHandler;
//import org.tudresden.unicore.knime.UnicoreClusterJob;
import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettings;
//import org.tudresden.unicore.knime.executor.settings.UnicorePreferenceInitializer;
import org.tudresden.unicore.knime.executor.settings.UnicorePreferenceInitializer;

/**
 * description for remote files being handled by unicore
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 *
 */
public class UnicoreRemoteFileHandler implements RemoteFileHandler<UnicoreConnection> {

	private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreExecutorHandler.class);
	public static final Protocol PROTOCOL = new Protocol("u6", 80, false, false, false, false, true, true, true, false);
	//public static final Protocol PROTOCOL8080 = new Protocol("u6", 8080, false, false, false, false, false, false, false, false);
	
	@Override
	public Protocol[] getSupportedProtocols() {
		return new Protocol[] {PROTOCOL};
	}

	@Override
	public RemoteFile<UnicoreConnection> createRemoteFile(URI uri, ConnectionInformation connectionInformation,
			ConnectionMonitor<UnicoreConnection> connectionMonitor) throws Exception {
		if(!(connectionInformation instanceof UnicoreConnectionInformation)) {
			LOGGER.error("Unicore File Handler can only handle u6 protocols");
		}
        UnicoreJobManagerSettings prefs = new UnicoreJobManagerSettings();
        UnicorePreferenceInitializer.getSettingsFromPreferences(prefs);	
		final UnicoreRemoteFile remoteFile = new UnicoreRemoteFile(uri, connectionInformation, connectionMonitor);
		return remoteFile;
	}

}
