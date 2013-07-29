package org.apache.hadoop.mapred.nativetask.testframe.util;

import org.apache.hadoop.conf.Configuration;

public class ScenarioConfiguration {
	public static final String common_conf_path="common_conf.xml";
	public static Configuration commonconf = new Configuration();
	static{
		commonconf.addResource(common_conf_path);
	}
	
}
 