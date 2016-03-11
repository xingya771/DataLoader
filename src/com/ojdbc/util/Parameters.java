package com.ojdbc.util;

/**
 * Created by Arthur on 2016/3/2.
 */

import org.apache.log4j.Logger;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * 存放程序用到的参数，以及线程用到的队列及任务完成标志
 *
 * @author Shengxingya <br/>
 *         Created time: 2013-11-15 上午10:30:08
 */
public class Parameters {
    public static final Properties properties = new Properties();
    /**
     * 日志
     */
    private static Logger logger = Logger.getLogger(Parameters.class);
    public static String fileSplit="\t";

    /**
     * 初始化参数，根据参数配置文件初始化各变量
     */
    public static void init(String basePath) {
        logger.debug("basePath:"+basePath);
        String path = null;
        // 通过参数的形式设置主目录，如果没设置此参数将尝试通过拼装类文件路径方式获取主目录
        if (!"".equals(basePath)) {
            path = basePath;
        } else {
            path = Parameters.class.getResource("/").getPath();
            if (path.endsWith("bin/")) {
                path = path.substring(0, path.length() - 4);
            }
        }

        path = path.replaceAll("%20", " ");
        path = path + "/conf/conf.properties";
        logger.debug("开始读入配置文件，配置文件路径为：" + path);
        File file = new File(path);
        FileInputStream fin = null;
        try {
            fin = new FileInputStream(file);
            properties.load(fin);
        } catch (IOException e) {
            e.printStackTrace();
            logger.error("载入配置文件时出现异常", e);
        } finally {
            try {
                if (fin != null) {
                    fin.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        initFileSplit();
        logger.debug("配置文件读入完毕");
    }

    /**
     * 获取配置文件中的参数值
     *
     * @param name          参数名
     * @param defaultValues 默认值，可不传
     * @return
     */
    public static String getParameter(String name, String... defaultValues) {
        if (defaultValues == null || defaultValues.length <= 0) {
            return properties.getProperty(name);
        } else {
            return properties.getProperty(name, defaultValues[0]);
        }

    }
    public static String $(String name, String... defaultValues){
        return getParameter(name,defaultValues);
    }


    public static void initFileSplit(){
        //分隔符,1:逗号，2：分号，3：Tab，4：空格，其他直接输入
        fileSplit=$("datafile.split", "1");
        switch (fileSplit){
            case "1":
                fileSplit=",";
                break;
            case "2":
                fileSplit=";";
                break;
            case "3":
                fileSplit="\t";
                break;
            case "4":
                fileSplit=" ";
                break;
            default:
                break;
        }
    }
}
