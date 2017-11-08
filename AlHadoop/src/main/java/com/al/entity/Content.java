package com.al.entity;

import java.util.HashSet;
import java.util.Set;

public class Content implements java.io.Serializable {
	private long contentId;
	private int dimeId;
	private String day;
	private int second;
	private String url;
	private String title;
	private int pv;
	private int uv;
	private Set<String> uuids;
	
	public void addReuqest(String uuid) {
		pv++;
		if(uuids == null) {
			uuids = new HashSet<String>();
		}
		uuids.add(uuid);
		uv = uuids.size();
	}

	public long getContentId() {
		return contentId;
	}
	public void setContentId(long contentId) {
		this.contentId = contentId;
	}
	public int getDimeId() {
		return dimeId;
	}
	public void setDimeId(int dimeId) {
		this.dimeId = dimeId;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public int getSecond() {
		return second;
	}
	public void setSecond(int second) {
		this.second = second;
	}
	public String getUrl() {
		return url;
	}
	public void setUrl(String url) {
		this.url = url;
	}
	public String getTitle() {
		return title;
	}
	public void setTitle(String title) {
		this.title = title;
	}
	public int getPv() {
		return pv;
	}
	public void setPv(int pv) {
		this.pv = pv;
	}
	public int getUv() {
		return uv;
	}
	public void setUv(int uv) {
		this.uv = uv;
	}
	public Set<String> getUuids() {
		return uuids;
	}
	public void setUuids(Set<String> uuids) {
		this.uuids = uuids;
	}
	
}
