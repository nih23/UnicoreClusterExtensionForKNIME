package org.tudresden.unicore;

import org.eclipse.ui.plugin.AbstractUIPlugin;
import org.knime.core.node.NodeLogger;
import org.osgi.framework.BundleContext;

/**
 * The activator class controls the plug-in life cycle.
 *
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */
public class UnicoreActivator extends AbstractUIPlugin {

	/** The plug-in ID. */
    public static final String PLUGIN_ID = "org.tudresden.unicore";

    private static final NodeLogger LOGGER = NodeLogger.getLogger(UnicoreActivator.class);

    // The shared instance
    private static UnicoreActivator plugin;

    /** 
     * The constructor.
     */
    public UnicoreActivator() {
        // nothing to construct here
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void start(final BundleContext context) throws Exception {
        super.start(context);
        plugin = this;

       
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void stop(final BundleContext context) throws Exception {
        plugin = null;
        super.stop(context);
    }

    /**
     * Returns the shared instance.
     *
     * @return the shared instance
     */
    public static UnicoreActivator getDefault() {
        return plugin;
    }

}
