可以分析binlog 的一些信息比如：
1、本binlog中是否有长期未提交的事物 
2、本binlog中是否有大事物 
3、本binlog中每个表生成了多少binlog 
4、本binlog中binlog生成速度。

./infobin  mysql-bin.001793 20 2000000 10 -t >log1793.log

第一个20 是分片数量,将binlog分为大小相等的片段，生成时间越短则这段时间生成binlog量越大，则事物越频繁。
第二个2000000 是大于2M左右的事物定义为大事物。
第三个10 是大于10秒未提交的事物定义为长期未提交的事物。 
第四个-t 代表不做详细event解析输出，仅仅获取相应的结果


只能用于binlog 不能用于relaylog。最好将binlog拷贝其他机器执行，不要在生产服务器跑
最好是5.6 5.7 row格式binlog

下面是详细的用法：

[root@gp1 infobin]# ./infobin 
USAGE ERROR!
[Author]: gaopeng [QQ]:22389860 [blog]:http://blog.itpub.net/7728585/ 
--USAGE:./infobin binlogfile pieces bigtrxsize bigtrxtime [-t] [-force]
[binlogfile]:binlog file!
[piece]:how many piece will split,is a Highly balanced histogram,
        find which time generate biggest binlog.(must:piece<2000000)
[bigtrxsize](bytes):larger than this size trx will view.(must:trx>256(bytes))
[bigtrxtime](sec):larger than this sec trx will view.(must:>0(sec))
[[-t]]:if [-t] no detail is print out,the result will small
[[-force]]:force analyze if unkown error check!!
