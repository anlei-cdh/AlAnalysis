package com.al.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;

/**
 * Created by An on 2016/12/7.
 */
public class DataCleaning {

    private static String [] searchEngines = {"百度","百度","百度","百度","360搜索","360搜索","Google","微软必应(Bing)","搜狐搜狗"};

    public static void cleaning(String input, String output, int start,int end, int dayindex) {
        try {
            FileUtil.deleteFile(output);
            FileReader fr = new FileReader(input);
            FileWriter fw = new FileWriter(output);
            BufferedReader buffReader = new BufferedReader(fr);
            BufferedWriter buffWriter = new BufferedWriter(fw);
            String line = null;
            int index = 0;
            int index_ip = 100;
            int index_uid = 100;
            int index_uuid = 100;

            while((line=buffReader.readLine()) != null) {
                if(index <= start) {
                    // System.out.println("index-start: " + index + " - " + start);
                    index++;
                    continue;
                }
                // ip cookie act 等用户信息清洗
                line = line.replaceAll("\"Act\":\"\\w+\",","\"Act\":\"aura\",");
                line = line.replaceAll("\"Ip\":\"\\d+\",","\"Ip\":\"" + index_ip + "\",");
                line = line.replaceAll("\"Uid\":\"\\w+\",","\"Uid\":\"" + index_uid + "\",");
                line = line.replaceAll("\"Uuid\":\"\\w+\",","\"UUid\":\"" + index_uuid + "\",");
                line = line.replaceAll("\"a\":\"\\W+\",","");
                line = line.replaceAll("\"ori\":\"\\W+\",","");
                line = line.replaceAll("\"md\":\"\\W+\",","");

                // 国家地区清洗
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"江苏\"","\"Country\":\"美国\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"香港\"","\"Country\":\"德国\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"河北\"","\"Country\":\"俄罗斯\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"陕西\"","\"Country\":\"澳大利亚\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"重庆\"","\"Country\":\"巴西\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"贵州\"","\"Country\":\"英国\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"湖北\"","\"Country\":\"加拿大\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"上海\"","\"Country\":\"日本\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"江西\"","\"Country\":\"印度\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"山西\"","\"Country\":\"哈萨克斯坦\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"天津\"","\"Country\":\"马来西亚\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"广西\"","\"Country\":\"南非\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"台湾\"","\"Country\":\"阿尔及利亚\",\"Area\":\"国外\"");
                line = line.replaceAll("\"Country\":\"中国\",\"Area\":\"吉林\"","\"Country\":\"苏丹\",\"Area\":\"国外\"");

                // 搜索引擎清洗
                if(!line.contains("\"SearchEngine\"")) {
                    String searchEngine = searchEngines[index % searchEngines.length];
                    // line = line.replaceAll("\"TimeZone\":\\d+","\"SearchEngine\":\"" + searchEngine + "\"");
                }

                // 换行符优化
                if(index != 0 && index != (start + 1)) {
                    buffWriter.newLine();
                }
                buffWriter.write(line);
                index++;

                if(dayindex == 1) {
                    if (index % 3 == 0 || index % 7 == 0) {
                        index_ip++;
                    }
                    if (index % 3 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                } else if(dayindex == 3) {
                    if (index % 3 == 0) {
                        index_ip++;
                    }
                    if (index % 2 == 0 || index % 5 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                } else if (dayindex == 4) {
                    if (index % 2 == 0 || index % 5 == 0) {
                        index_ip++;
                    }
                    if (index % 3 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                } else if (dayindex == 6) {
                    if (index % 2 == 0 || index % 11 == 0) {
                        index_ip++;
                    }
                    if (index % 2 == 0 || index % 3 == 0 || index % 5 == 0 || index % 7 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                } else if (dayindex == 7) {
                    if (index % 2 == 0 || index % 5 == 0) {
                        index_ip++;
                    }
                    if (index % 6 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                } else {
                    if (index % 2 == 0) {
                        index_ip++;
                    }
                    if (index % 3 == 0) {
                        index_uid++;
                        index_uuid++;
                    }
                }
                if(index >= end) {
                    System.out.println("index-end: " + index + " - " + end);
                    break;
                }
            }
            buffReader.close();
            buffWriter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        cleaning("D:/logs/basic.log", "logs/aura.log", 0, 10000, 0);
        cleaning("D:/logs/basic.log", "logs/aura20161201.log", 0, 800, 1); // 800
        cleaning("D:/logs/basic.log", "logs/aura20161202.log", 800, 2000, 2); // 1200
        cleaning("D:/logs/basic.log", "logs/aura20161203.log", 2000, 3600, 3); // 1600
        cleaning("D:/logs/basic.log", "logs/aura20161204.log", 3600, 5000, 4); // 1400
        cleaning("D:/logs/basic.log", "logs/aura20161205.log", 5000, 6100, 5); // 1100
        cleaning("D:/logs/basic.log", "logs/aura20161206.log", 6100, 6880, 6); // 780
        cleaning("D:/logs/basic.log", "logs/aura20161207.log", 6880, 8000, 7); // 1120
    }
}
