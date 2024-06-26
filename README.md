# Real_Time_Data_Warehouse
**写在前面**：  
在完成电商的离线数仓的搭建后，开始学习有关Flink的知识，一段时间的学习后，决定通过做一个实时的项目来巩固知识，加深理解，希望有新的收获！！！！
## 电商实时数仓：
*开始时间*：2024-03-07  
*项目简介*：
此项目沿用之前离线数仓的来源数据，采取新的数据架构，将数据导入kafka中，通过flink引擎来处理数据，搭建一个实时数仓，使得部分数据的处理更加及时。

### 子模块common：公共类的复用设计
1. **前置公共类的设计**  
Flink Job的处理流程大致可以分为以下几步：  
（1）初始化流处理环境，配置检查点，从Kafka中读取目标主题数据  
（2）执行核心处理逻辑  
（3）执行  
【注】其中，所有Job的第一步和第三步基本相同，我们可以定义基类，将这两步交给基类完成。定义抽象方法，用于实现核心处理逻辑，子类只需要重写该方法即可实现功能。
省去了大量的重复代码，且不必关心整体的处理流程。
2. **包的具体作用**  
base:  启动flink程序的入口基类  
bean:  TableProcessDim类封装了维度表的信息。  
constant（常量包）:  kafka，mysql，hbase相关信息  
function:  
util:  
   1. FlinkSourceUtil_kafka:将kafka作为flink的数据源
   2. HbaseUtil:提供操作hbase的方法：包括hbase的连接，关闭，创建表，删除表，插入数据（在DimApp中），删除数据（在DimApp中），查询数据（未开发）等
   3. JDBCUtil:提供操作mysql的方法：包括mysql的连接，关闭，查询数据等  
### 子模块real_gmall_dim:维度层的搭建
1.**维度层的设计**:  
选用hbase数据库作为dim层容器，配合mysql配置表和flink-cdc实现维度数据的动态冷同步。因为hbase是一个分布式的NoSQL数据库，具有高可用性，高可扩展性，高容错性，高可读写性能。  
2.**维度层的搭建**:  
- 读取ods层数据，将其作为主数据流。
- 同步配置表中信息作为配置流。
- 将主数据流和配置流进行join，将维度数据同步到hbase中。

