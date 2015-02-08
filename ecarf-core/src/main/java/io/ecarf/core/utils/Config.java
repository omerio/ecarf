/**
 * The contents of this file may be used under the terms of the Apache License, Version 2.0
 * in which case, the provisions of the Apache License Version 2.0 are applicable instead of those above.
 *
 * Copyright 2014, Ecarf.io
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.ecarf.core.utils;

import java.io.IOException;
import java.net.URL;
import java.util.List;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Configurations management Utility
 * 
 * @author Omer Dawelbeit (omerio)
 *
 */
public class Config {

	private final static Log log = LogFactory.getLog(Config.class);

	/**
	 * <p>The default configuration file</p>
	 */
	private static final String DEFAULT_CONFIG_FILE = "config.properties";

	/**
	 * <p>The default list delimiter used for list properties (default is ,)</p>
	 */
	private static final char LIST_DELIMITER = ';';

	/**
	 * private singleton instance
	 */
	private static volatile Config singleton;

	//private static Properties properties = new Properties();
	private static PropertiesConfiguration config = new PropertiesConfiguration();

	/**
	 * private constructor to reinforce the singleton pattern
	 * @throws IOException 
	 * @throws ConfigurationException 
	 */
	private Config() throws ConfigurationException	{
		super ();
		this.loadPropertiesFromFile(DEFAULT_CONFIG_FILE);
	}

	/**
	 * returns the sigleton
	 * 
	 * @return
	 * @throws IOException 
	 * @throws ConfigurationException 
	 */
	protected static Config getInstance()	{

		if (singleton == null)	{
			synchronized (config)	{
				// check if still null in case another thread is still waiting
				if (singleton == null)	{

					try {
						singleton = new Config();
					} catch (ConfigurationException e) {
						log.error("Failed to initialize Config instance", e);
						throw new RuntimeException(e);
					}
				}
			}
			log.debug("Successfully created Config Singleton");
		}

		return singleton;
	}

	/**
	 * <p>Get a Property object from a file</p>
	 * 
	 * @param fileName
	 * @return
	 * @throws IOException 
	 * @throws ConfigurationException 
	 */
	private void loadPropertiesFromFile(String fileName) throws ConfigurationException	{
		
		URL url = Thread.currentThread().getContextClassLoader().getResource(fileName);

		config.setListDelimiter(LIST_DELIMITER);
		config.setURL(url);
		config.load();
		
	}

	/**
	 * <p>Return a property for the provided key</p>
	 * @param key
	 * @return
	 */
	public static String getProperty(String key)	{

		return getInstance().getConfig().getString(key);
	}

	/**
	 * <p>Return a property for the provided key</p>
	 * @param key
	 * @return
	 */
	public static String getProperty(String key, String defaultValue)	{
		
		return getInstance().getConfig().getString(key, defaultValue);
	}

	
	/**
	 * <p>Return a property for the provided key</p>
	 * @param key
	 * @return
	 */
	public static Integer getIntegerProperty(String key, Integer defaultValue)	{

		return getInstance().getConfig().getInteger(key, defaultValue);
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static Double getDoubleProperty(String key, Double defaultValue)	{

		return getInstance().getConfig().getDouble(key, defaultValue);
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static Long getLongProperty(String key, Long defaultValue)	{

		return getInstance().getConfig().getLong(key, defaultValue);
	}

	/**
	 * @param key
	 * @param defaultValue
	 * @return
	 */
	public static Boolean getBooleanProperty(String key, Boolean defaultValue)	{

		return getInstance().getConfig().getBoolean(key, defaultValue);
	}

	/**
	 * Get a list from the properties file<br>
	 * properties like this<br>
	 * key = This property, has multiple, values
	 * <br> will be put in a list
	 * @param key
	 * @return
	 */
	@SuppressWarnings("rawtypes")
	public static List getList(String key) {
		return getInstance().getConfig().getList(key);
	}

	/**
	 * @return the properties
	 */
	protected PropertiesConfiguration getConfig() {
		return config;
	}

	/**
	 * Sets a new file name for reading the config, used for testing
	 * @param fileName
	 * @throws ConfigurationException 
	 */
	/*public static synchronized void setConfigFile(String fileName) throws ConfigurationException	{
		config.setFileName(fileName);
		config.refresh();
	}*/

}
