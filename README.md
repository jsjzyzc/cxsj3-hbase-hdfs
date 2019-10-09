# hdfs-demo
使用Java API操作HDFS分布式文件系统

1. demo概述
   + 该demo是用SpringBoot框架和Java语言编写的操作HDFS的纯后端服务，提供了相关接口；
   + 该demo集成了Swagger框架，项目启动成功后访问<http://localhost:8080/swagger-ui.html#/>即可查看项目对应的Swagger文档；
   
2. Java操作HDFS主要涉及以下几个重要的类
   + Configuration：封装了客户端或者服务器的配置信息；
   + FileSystem：此类的对象是一个文件系统对象，可以用该对象的一些方法来对文件进行操作通过FileSystem的静态方法get获得该对象，例：FileSystem hdfs = FileSystem.get(conf)；
   + FSDataInputStream：这是HDFS中的输入流，通过由FileSystem的open方法获取；
   + FSDataOutputStream：这是HDFS中的输出流，通过由FileSystem的create方法获取。


  
