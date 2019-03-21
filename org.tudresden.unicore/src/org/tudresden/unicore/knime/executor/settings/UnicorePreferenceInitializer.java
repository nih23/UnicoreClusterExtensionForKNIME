package org.tudresden.unicore.knime.executor.settings;

import java.io.File;

import org.eclipse.core.runtime.preferences.AbstractPreferenceInitializer;
import org.eclipse.jface.preference.IPreferenceStore;
import org.knime.core.node.NodeLogger;
import org.tudresden.unicore.UnicoreActivator;


/**
 * default values for our settings ..
 * @author Nico Hoffmann, TU Dresden (nico.hoffmann@tu-dresden.de)
 */

public class UnicorePreferenceInitializer extends AbstractPreferenceInitializer {

	 /** Preference key: knime executable. */
    static final String PREF_KNIME_EXEC =
            "com.knime.cluster.unicore.jobmanager.knime_executable";

    /** Preference key: knime arguments. */
    static final String PREF_KNIME_ARGS =
            "com.knime.cluster.unicore.jobmanager.knime_arguments";
    
    /** Preference key: native arguments. */
    /*static final String PREF_NATIVE_ARGS =
            "com.knime.cluster.unicore.jobmanager.unicore_native_args";*/

    /** Preference key: shared directory (local path). */
    static final String PREF_LOCAL_SHARED =
            "com.knime.cluster.unicore.jobmanager.local_shared";

    /** Preference key: shared directory (grid machine path). */
    static final String PREF_CLUSTER_SHARED =
            "com.knime.cluster.unicore.jobmanager.cluster_shared";

    /** Preference key: export client preferences onto grid machines. */
    static final String PREF_EXPORT_CLIENT_PREFERENCES =
            "com.knime.cluster.unicore.jobmanager.export_client_preferences";

    /** Preference key: shell for invoke script (either bash or tcsh) */
    static final String PREF_INVOKE_SHELL =
            "com.knime.cluster.unicore.jobmanager.invoke_shell";
    
    /** Preference key: unicore username */
    static final String PREF_UNICORE_USERNAME =
            "com.knime.cluster.unicore.jobmanager.username";
    
    /** Preference key: unicore password */
    static final String PREF_UNICORE_PASSWORD =
            "com.knime.cluster.unicore.jobmanager.password";
    
    /** Preference key: unicore gateway */
    static final String PREF_UNICORE_GATEWAY =
            "com.knime.cluster.unicore.jobmanager.gateway";
    
    /** Preference key: unicore storage */
    static final String PREF_UNICORE_STORAGE =
            "com.knime.cluster.unicore.jobmanager.storage";
    
    /** Preference key: job runtime */
    static final String PREF_UNICORE_RESOURCES_RUNTIME =
            "com.knime.cluster.unicore.jobmanager.resources.runtime";
    
    /** Preference key: job memory */
    static final String PREF_UNICORE_RESOURCES_MEMORY =
            "com.knime.cluster.unicore.jobmanager.resources.memory";
    
    /** Preference key: job nodes */
    static final String PREF_UNICORE_RESOURCES_NODES =
            "com.knime.cluster.unicore.jobmanager.resources.nodes";
    
    /** Preference key: job CPUs per Node */
    static final String PREF_UNICORE_RESOURCES_CPUsPerNode =
            "com.knime.cluster.unicore.jobmanager.resources.cpuspernode";
    
    /** Preference key: unicore default sitename */
    static final String PREF_UNICORE_DEFAULT_SITENAME =
            "com.knime.cluster.unicore.jobmanager.default_sitename";

    /** Preference key: temp file delete policy. */
    static final String PREF_DELETE_TEMP_FILE_POLICY =
            "com.knime.cluster.unicore.jobmanager.delete_temp_file_policy";

    /**
     * Returns the preference store used for the SGE job manager preferences.
     * Currently we use KNIME's store...
     *
     * @return the preference store used for the SGE job manager preferences.
     */
    public static IPreferenceStore getUnicorePreferenceStore() {
        return UnicoreActivator.getDefault().getPreferenceStore();
    }

    /**
     * Write the values currently stored in the preference store into the passed
     * settings object, overriding only the values that are specified in the
     * preference page.
     *
     * @param settings the settings object to write the current preference page
     *            values into
     *
     */
    public static void getSettingsFromPreferences(
            final UnicoreJobManagerSettings settings) {

        IPreferenceStore store = getUnicorePreferenceStore();
        /*settings.setRemoteExecDir(store.getString(PREF_CLUSTER_SHARED));
        //settings.setCustomKnimeArguments(ClusterJobSplitSettings
        //        .splitArgumentString(store.getString(PREF_KNIME_ARGS)));
        //settings.setRemoteKnimeExecutable(store.getString(PREF_KNIME_EXEC));
        //settings.setLocalRootDir(new File(store.getString(PREF_LOCAL_SHARED)));
        settings.setStorage(store.getString(PREF_UNICORE_STORAGE));
        settings.setDefaultSitename(store.getString(PREF_UNICORE_DEFAULT_SITENAME));
        settings.setUsername(store.getString(PREF_UNICORE_USERNAME));
        settings.setPassword(store.getString(PREF_UNICORE_PASSWORD));
        settings.setGateway(store.getString(PREF_UNICORE_GATEWAY));
        settings.setJobCPUsPerNode(store.getString(PREF_UNICORE_RESOURCES_CPUsPerNode));
        settings.setJobRuntime(store.getString(PREF_UNICORE_RESOURCES_RUNTIME));
        settings.setJobMemory(store.getString(PREF_UNICORE_RESOURCES_MEMORY));
        settings.setJobNodes(store.getString(PREF_UNICORE_RESOURCES_NODES));*/
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeDefaultPreferences() {

        // get the preference store for SGE job manager
        IPreferenceStore store = getUnicorePreferenceStore();

        // set default values
        store.setDefault(PREF_UNICORE_STORAGE, "default_storage");
        store.setDefault(PREF_UNICORE_RESOURCES_CPUsPerNode, "12");
        store.setDefault(PREF_UNICORE_RESOURCES_NODES, "1");
        store.setDefault(PREF_UNICORE_RESOURCES_MEMORY, "4G");
        store.setDefault(PREF_UNICORE_RESOURCES_RUNTIME, "10min");
        store.setDefault(PREF_UNICORE_USERNAME, "");
        store.setDefault(PREF_UNICORE_PASSWORD, "");
        store.setDefault(PREF_UNICORE_GATEWAY, "");
        store.setDefault(PREF_UNICORE_DEFAULT_SITENAME, "");
        store.setDefault(PREF_CLUSTER_SHARED, "/");
        store.setDefault(PREF_KNIME_ARGS, "");
        store.setDefault(PREF_KNIME_EXEC, "");
        store.setDefault(PREF_LOCAL_SHARED, "");
        store.setDefault(PREF_EXPORT_CLIENT_PREFERENCES, true);
    }

}
