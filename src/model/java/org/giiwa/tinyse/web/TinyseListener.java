package org.giiwa.tinyse.web;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.giiwa.framework.web.IListener;
import org.giiwa.framework.web.Module;
import org.giiwa.tinyse.se.SE;

public class TinyseListener implements IListener {

	static Log log = LogFactory.getLog(TinyseListener.class);

	@Override
	public void onStart(Configuration conf, Module m) {
		// TODO Auto-generated method stub
		log.info("Tinyse is starting ...");

		SE.init(conf);
	}

	@Override
	public void onStop() {
		// TODO Auto-generated method stub

	}

	@Override
	public void uninstall(Configuration arg0, Module arg1) {
		// TODO Auto-generated method stub

	}

	@Override
	public void upgrade(Configuration arg0, Module arg1) {
		// TODO Auto-generated method stub

	}

}
