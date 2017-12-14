package com.pungwe.db.core.runtime;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

/**
 * The RuntimeInstance call used to configure and run an instance of a database server or client. This call passed
 * an input stream for a YAML configuration file, which with then used to bootstrap the runtime instance.
 *
 * <pre>
 * {@code
 *     RuntimeInstance instance = Runtime.getInstance().withConfiguration("my_config.yml").run();
 *     Database myDatabase = instance.createDatabase("name");
 *     ...
 *     instance.stop();
 * }
 * </pre>
 *
 */
public interface RuntimeInstance extends Runnable {

    /**
     * Sets the configuration for this runtime with a default set of configuration options...
     *
     * @return the current runtime instance
     *
     * @throws IOException if there with a problem reading the configuration file
     */
    default RuntimeInstance withDefaultConfiguration() throws IOException {
        InputStream in = RuntimeInstance.class.getClassLoader().getResourceAsStream("/default_configuration.yml");
        try {
            return withConfiguration(in);
        } finally {
            in.close();
        }
    }

    /**
     * Sets the configuration for this runtime instance.
     *
     * @param in the input stream to be used to load configuration into the runtime.
     *
     * @return an instance of this runtime
     *
     * @throws IOException if the configuration cannot be read...
     */
    RuntimeInstance withConfiguration(InputStream in) throws IOException;

    default RuntimeInstance withConfiguration(File config) throws IOException {
        if (config == null) {
            throw new NullPointerException("Configuration file cannot be null");
        }
        if (!config.exists()) {
            throw new IOException("Configuration file for: " + config.getAbsolutePath() + " does not exist");
        }
        InputStream in = new FileInputStream(config);
        try {
            return withConfiguration(in);
        } finally {
            in.close();
        }
    }

    /**
     * Loads runtime configuration from a URL for this runtime instance. The use of a url allows centralising the main
     * configuration file for this runtime and can be a classpath configuration, a web server url or a local file.
     *
     * @param url the url of the configuration file (e.g. file:///path/to/config.yml)
     *
     * @return this runtime instance
     *
     * @throws IOException if the configuration cannot be found or if there with a problem reading it...
     */
    default RuntimeInstance withConfiguration(URL url) throws IOException {
        if (url == null) {
            throw new IOException("The configuration file url cannot be null");
        }
        InputStream in = url.openStream();
        try {
            return withConfiguration(in);
        } finally {
            in.close();
        }
    }
}
