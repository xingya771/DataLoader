package com.ojdbc.dataloader;

import com.ojdbc.util.DBUtil;
import com.ojdbc.util.Parameters;
import org.apache.log4j.Logger;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.ParseException;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

import static com.ojdbc.util.Parameters.fileSplit;

/**
 * 就数据保存到数据库表中
 * Created by Arthur on 2016/3/11.
 */
public class Loader extends Thread {
    /**
     * 批量提交行数
     */
    private int limit = Integer.parseInt(Parameters.$("datafile.commitlimit", "1000"));
    private Logger logger = Logger.getLogger(Loader.class);
    public BlockingQueue<String> queue = new ArrayBlockingQueue<String>(
            limit * 2);
    private String tName;
    private String title[];
    public boolean runFlag;
    public long commitCount;
    public long successCount;
    public boolean isComplete;

    public Loader(String tName) {
        this.tName = tName;
        this.setName("LoaderThread[" + tName + "]");
        init();
    }

    public void setTitle(String title[]) {
        this.title = title;
    }

    @Override
    public void run() {
        try {
            insert2DB();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }

    /**
     * 插入到数据库
     *
     * @throws InterruptedException
     * @throws SQLException
     * @throws ParseException
     */
    public void insert2DB() throws InterruptedException, SQLException, ParseException {
        logger.debug("开始解析：" + this.tName);
        int count = 0;
        PreparedStatement ps = null;
        Connection conn = DBUtil.getConn();
        //从数据库获取表的列类型信息
        Map<String, String> map = DBUtil.getColsType_oracle(tName);
        //拼装供prepareStatement使用的sql
        ps = conn.prepareStatement(DBUtil.createPreparedStatementSql(tName, title, fileSplit));
        while (runFlag) {
            String queue_line = queue.poll();
            if (queue_line == null) {
                continue;
            }
            String lines[] = queue_line.split(fileSplit);
            count += 1;
            for (int i = 0; i < title.length; i++) {
                if (i >= lines.length) {
                    ps.setString(i + 1, "");
                    break;
                }
                String colValues = lines[i];
                if ("".equals(colValues)) {
                    ps.setString(i + 1, "");
                    continue;
                }

                DBUtil.setPSValues(map.get(title[i].toUpperCase()), ps, i, colValues);
            }
            ps.addBatch();
            commitCount += 1;
            if (count == limit) {
                int[] res = ps.executeBatch();
                successCount += DBUtil.getBatchCommitSuccessCount(res);
                logger.debug("提交" + limit + "行，共成功" + successCount + "行");
                count = 0;
            }

        }
        int[] res = ps.executeBatch();
        successCount += DBUtil.getBatchCommitSuccessCount(res);
        logger.debug("入库完成，共成功" + successCount + "行");
        conn.close();
        isComplete = true;
    }

    public void init() {
        isComplete = false;
        commitCount = 0;
        successCount = 0;
        runFlag = true;
    }
}
