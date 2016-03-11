package com.ojdbc.dataloader;

import com.ojdbc.util.DBUtil;
import com.ojdbc.util.Parameters;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Map;

import static com.ojdbc.util.Parameters.fileSplit;

/**
 * Created by Arthur on 2016/3/10.
 */
public class Parser extends Thread {
    private Logger logger = Logger.getLogger(Parser.class);
    private String dateFormat = Parameters.$("datafile.dateFormat");
    private Loader loader;
    private long fileLineCount;
    /**
     * 批量提交行数
     */
    private int limit = Integer.parseInt(Parameters.$("datafile.commitlimit", "1000"));

    public Parser(String name) {
        this.setName("ParserThread[" + name + "]");
    }

    @Override
    public void run() {
        while (Main.hasMoreFilesFlag) {
            try {
                File file = Main.queue.take();
                loader = new Loader(DBUtil.getTableNameFromFileName(file));
                parseFile(file);
                logger.debug("文件："+file.getName()+"已解析完毕");
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ParseException e) {
                e.printStackTrace();
            } catch (IOException e) {
                e.printStackTrace();
            } catch (SQLException e) {
                e.printStackTrace();
            }catch (Exception e){
                e.printStackTrace();
            }

        }
    }

    /**
     * 解析文件按行放入对应表的解析线程队列里
     *
     * @param file 数据文件
     * @throws SQLException
     * @throws IOException
     * @throws ParseException
     */
    private void parseFile(File file) throws SQLException, IOException, ParseException, InterruptedException {
        long now = System.currentTimeMillis();
        fileLineCount = 0;
        String tName = DBUtil.getTableNameFromFileName(file);
        logger.debug("insert to " + tName + " begin");

        //使用Apache的commons-io中的相关API读取文件
        LineIterator it = FileUtils.lineIterator(file);
        String title = "";
        Map<String, String> map = DBUtil.getColsType_oracle(tName);
        if (Parameters.$("datafile.hasTitle").equals("true")) {
            title = it.nextLine();
        } else {
            title = Parameters.$("datafile.titleCols");
        }

        if (!",".equals(fileSplit)) {
            title = title.trim().replace(fileSplit, ",");
        }

        if (title.endsWith(",")) {
            title = title.trim().substring(0, title.length() - 1);
        }
        String cols[] = title.split(",");
        logger.debug("表头："+title);
        loader.setTitle(cols);
        loader.start();

        while (it.hasNext()) {
            loader.queue.put(it.nextLine());
            fileLineCount += 1;
        }
        logger.debug("文件解析完成，文件名为："+file.getName());

        //解析完文件后等待，因入库线程是批量提交，故判断文件行数-已提交行数小于等于批量提交行数的话等待
        while (fileLineCount - loader.commitCount >= limit) {
            Thread.sleep(1000);
        }
        loader.runFlag = false;
        //等待直到入库线程结束
        while(!loader.isComplete){
            Thread.sleep(1000);
        }

        logger.debug("表" + tName + "导入完成，数据文件行数：" + fileLineCount + "；成功插入:"+loader.commitCount);

    }
}
