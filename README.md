# DataLoader
从文本文件**abc.xxx**中（以逗号、tab分隔的）读取数据，然后导入到数据库中的**abc**表中。<br/>
1. 列名可以在**abc.xxx**中以标题行的形式存在，或者在conf/conf.properties中配置<br/>
2. 分隔符可以自定义（在conf/conf.properties中修改**datafile.split**）<br/>
    预定义的几种分隔符：1:逗号，2：分号，3：Tab，4：空格
3. 支持多线程解析入库