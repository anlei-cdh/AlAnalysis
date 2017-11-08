package com.al.entity;

import java.util.HashSet;
import java.util.Set;

public class Dimension implements java.io.Serializable {

	private int dimeId;
	private String day;
	private int second;
	private String type;
	private String value;
	private int pv;
	private int uv;
	private int ip;
	private long time;
	private Set<String> uuids;
	
	public void addReuqest(String uuid) {
		pv++;
		if(uuids == null) {
			uuids = new HashSet<String>();
		}
		uuids.add(uuid);
		uv = uuids.size();
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
	public String getType() {
		return type;
	}
	public void setType(String type) {
		this.type = type;
	}
	public String getValue() {
		return value;
	}
	public void setValue(String value) {
		this.value = value;
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
	public int getIp() {
		return ip;
	}
	public void setIp(int ip) {
		this.ip = ip;
	}
	public long getTime() {
		return time;
	}
	public void setTime(long time) {
		this.time = time;
	}
}
