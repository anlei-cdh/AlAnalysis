package com.al.entity;

import com.al.util.StringUtil;

public class Log {
	private String Act;
	private long Ts;
	private String Ip;
	private String Uuid;
	private String Uid;
	private String ContentId;
	private String Url;
	private String Title;
	private String Refer;
	private String Ua;
	private String Country;
	private String Area;
	private Wd wd;
	private char pagetype;

	/**
	 * 日志是否合法
	 * @return
	 */
	public boolean isLegal() {
		if(Ip == null || Ip.equals("") || Uuid == null || Uuid.equals("")) {
			return false;
		}
		return true;
	}
	
	public char getPagetype() {
		Wd wd = getWd();
		if(wd != null && wd.getT() != null) {
			String wdt = wd.getT();
			if(wdt.length() == 3) {
				char pt = wdt.charAt(2);
				if(pt >= '0' && pt <= '5') {
					pagetype = pt;
				}
			}
		}
		return pagetype;
	}

	public String getClearTitle() {
		String title = "";
		if(Title != null) {
			title = StringUtil.clearTitleAll(Title);
		}
		return title;
	}
	
	class Wd {
		String t;

		public String getT() {
			return t;
		}
		public void setT(String t) {
			this.t = t;
		}
	}
	
	public String getAct() {
		return Act;
	}
	public void setAct(String act) {
		Act = act;
	}
	public long getTs() {
		return Ts;
	}
	public void setTs(long ts) {
		Ts = ts;
	}
	public String getIp() {
		return Ip;
	}
	public void setIp(String ip) {
		Ip = ip;
	}
	public String getUuid() {
		return Uuid;
	}
	public void setUuid(String uuid) {
		Uuid = uuid;
	}
	public String getUid() {
		return Uid;
	}
	public void setUid(String uid) {
		Uid = uid;
	}
	public String getContentId() {
		return ContentId;
	}
	public void setContentId(String contentId) {
		ContentId = contentId;
	}
	public String getUrl() {
		return Url;
	}
	public void setUrl(String url) {
		Url = url;
	}
	public String getTitle() {
		return Title;
	}
	public void setTitle(String title) {
		Title = title;
	}
	public String getRefer() {
		return Refer;
	}
	public void setRefer(String refer) {
		Refer = refer;
	}
	public String getUa() {
		return Ua;
	}
	public void setUa(String ua) {
		Ua = ua;
	}
	public String getCountry() {
		return Country;
	}
	public void setCountry(String country) {
		Country = country;
	}
	public String getArea() {
		return Area;
	}
	public void setArea(String area) {
		Area = area;
	}
	public Wd getWd() {
		return wd;
	}
	public void setWd(Wd wd) {
		this.wd = wd;
	}
	public void setPagetype(char pagetype) {
		this.pagetype = pagetype;
	}
	
}
