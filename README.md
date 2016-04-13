# DataLoader
从文本文件**abc.xxx**中（以逗号、tab分隔的）读取数据，然后导入到数据库中的**abc**表中。<br/>
1. 列名可以在**abc.xxx**中以标题行的形式存在，或者在conf/conf.properties中配置<br/>
2. 分隔符可以自定义（在conf/conf.properties中修改**datafile.split**）<br/>
       预定义的几种分隔符：1:逗号，2：分号，3：Tab，4：空格<br/>
3. 支持多线程解析入库<br/>
4. 要提高性能，可以修改conf/conf.properties中**datafile.commitlimit**的值。<br/>
       该值控制批量提交阈值，应根据自己的数据库性能调整该值。<br/>