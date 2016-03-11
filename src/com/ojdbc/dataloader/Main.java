package com.ojdbc.dataloader;

import com.ojdbc.util.DBUtil;
import com.ojdbc.util.Parameters;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import java.io.File;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

/**
 * Created by Arthur on 2016/3/11.
 */
public class Main {
    public static boolean hasMoreFilesFlag = true;
    private static Logger logger = Logger.getLogger(Main.class);
    public static BlockingQueue<File> queue = new ArrayBlockingQueue<File>(
            8);
    private static int threadSize;
    private static boolean isLoadFile = false;

    public static void main(String[] args) {
        /**
         * 在Windows中可以使用 “%~dp0”,在Linux中可以使用"$(pwd)"来获取当前目录
         */
        String arg = null;
        if (args.length == 0 || null == args[0]) {
            arg = getPath();
        } else {
            arg = args[0];
        }
        if (arg.indexOf("/target/classes/") != -1) {
            arg = arg.substring(0, arg.length() - "/target/classes/".length());
        }
        PropertyConfigurator.configure(arg + "/conf/log4j.properties");
        Parameters.init(arg);
        getFileFromDir(Parameters.$("datafile.path", arg));
        threadSize=Integer.parseInt(Parameters.$("threadsize", "1"));
        for (int i = 0; i < threadSize; i++) {
            Parser parser = new Parser(String.valueOf(i));
            parser.start();
        }
        //等待读取文件名放入队列
        while (!isLoadFile) {
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.debug("读取数据文件完毕");

        //等待所有文件被解析线程解析
        while (!(queue.size() == 0)) {
            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        logger.debug("所有数据文件均已进入解析流程");

        hasMoreFilesFlag = false;
    }

    /**
     * @param path
     */
    public static void getFileFromDir(String path) {
        File dataPath = new File(path);

        if (!dataPath.isDirectory()) {
            logger.error("path not exists or not a directory");
            return;
        }
        File files[] = dataPath.listFiles();
        for (File file : files) {
            String tName = DBUtil.getTableNameFromFileName(file);
            queue.add(file);
        }
        isLoadFile = true;
    }

    public static String getPath() {
        return Main.class.getResource("/").getPath();
    }


}
