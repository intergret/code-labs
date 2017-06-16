# labs-curator
使用Zookeeper实现Leader Election、Queue、Barrier等功能和组件

### 要点：
1. **[LeaderElection](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_leaderElection)**：Master启动后都注册到/master路径下，创建临时的顺序节点。当前最老的Master成为Leader，Leader还将自己注册到/leader路径下。普通的Master只Watch前面一个Master的临时节点，避免羊群效应;
	
2. **[Queue](http://zookeeper.apache.org/doc/trunk/recipes.html#sc_recipes_Queues)**：Producer将所有的任务发布在/tasks路径下，Consumer获取/tasks路径下所有的任务，遍历每个任务消费时，需要先获得对应任务的消费锁，然后确认任务还存在，避免消息重复消费;