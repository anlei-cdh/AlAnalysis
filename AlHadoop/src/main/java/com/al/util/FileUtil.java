package com.al.util;

import java.io.File;

public class FileUtil {
	
	/**
	 * 检查文件
	 */
	public static boolean existsFile(String path) {
		if(path == null) {
			return false;
		}
		File file = new File(path);
		return file.exists();
	}
	
	/**
	 * 查看文件大小
	 */
	public static long getFileSize(String path) {
		if(path == null) {
			return 0;
		}
		File file = new File(path);
		return file.length();
	}
	
	/**
	 * 删除文件
	 * @param sPath
	 * @return
	 */
	public static boolean deleteFile(String sPath) {   
	    boolean flag = false;   
	    File file = new File(sPath);   
	    // 路径为文件且不为空则进行删除   
	    if (file.isFile() && file.exists()) {   
	        file.delete();   
	        flag = true;   
	    }   
	    return flag;   
	}
	
	/**
	 * 删除文件夹
	 * @param sPath
	 * @return
	 */
	public static boolean deleteDirectory(String sPath) {   
		 boolean flag = false;
	    //如果sPath不以文件分隔符结尾，自动添加文件分隔符   
	    if (!sPath.endsWith(File.separator)) {   
	        sPath = sPath + File.separator;   
	    }   
	    File dirFile = new File(sPath);   
	    //如果dir对应的文件不存在，或者不是一个目录，则退出   
	    if (!dirFile.exists() || !dirFile.isDirectory()) {   
	        return false;   
	    }   
	    flag = true;   
	    //删除文件夹下的所有文件(包括子目录)   
	    File[] files = dirFile.listFiles();   
	    for (int i = 0; i < files.length; i++) {   
	        //删除子文件   
	        if (files[i].isFile()) {   
	            flag = deleteFile(files[i].getAbsolutePath());   
	            if (!flag) break;   
	        } //删除子目录   
	        else {   
	            flag = deleteDirectory(files[i].getAbsolutePath());   
	            if (!flag) break;   
	        }   
	    }   
	    if (!flag) return false;   
	    //删除当前目录   
	    if (dirFile.delete()) {   
	        return true;   
	    } else {   
	        return false;   
	    }   
	}
	
	public static void main(String [] args) {
	}
}
