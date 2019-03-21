package org.tudresden.unicore.knime;

import org.knime.cluster.ClusterConfiguration;
import org.knime.cluster.ClusterFactory;
import org.tudresden.unicore.knime.executor.settings.UnicoreExecutorConfiguration;
import org.tudresden.unicore.knime.executor.settings.UnicoreJobManagerSettings;

public class UnicoreExecutorFactory implements ClusterFactory {

	@Override
	public ClusterConfiguration createClusterConfiguration() {
		return new UnicoreExecutorConfiguration();
	}

}
