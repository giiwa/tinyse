package org.giiwa.tinyse.web.admin;

import org.giiwa.framework.web.Model;
import org.giiwa.framework.web.Path;
import org.giiwa.tinyse.se.SE;

public class tinyse extends Model {

	@Path(path = "status", login = true, access = "access.config.admin")
	public void status() {
		this.set("types", SE.getTypes());
		this.set("m", this);
		this.show("/tinyse/status.html");
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public int count(String type) {
		return SE.count(type);
	}

	/**
	 * 
	 * @param type
	 * @return
	 */
	public int error(String type) {
		return SE.error(type);
	}

	public float index(String type) {
		return SE.index(type);
	}

	public float seach(String type) {
		return SE.search(type);
	}

	public long seachmax(String type) {
		return SE.searchmax(type);
	}
	public long seachmin(String type) {
		return SE.searchmin(type);
	}

}
