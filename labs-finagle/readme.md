# finagle-example
基于Thrift和Finagle RPC服务示例。Server端将服务注册到ZK，Client端维护Server节点列表，请求随机转发给其中的某个Server节点。

### 示例：
* 累加求和的RPC接口由Thrift定义，并使用Scrooge生成stub相关代码；

```
	service AddService {
	    i32 request(1: i32 num1, 2: i32 num2);
	}
```

* Server端将累加求和接口启在固定端口上，使用ExecutorServiceFuturePool处理请求，返回结果的Future；

```
	ExecutorServiceFuturePool futurePool = new ExecutorServiceFuturePool(executorService);
	
	public Future<Integer> request(final int num1, final int num2) {
	    return futurePool.apply(new Function0<Integer>() {
	      @Override
	      public Integer apply() {
	        try {
	          Thread.sleep(num1 + num2);
	        } catch (InterruptedException e) {
	          System.out.println(e);
	        }
	        return num1 + num2;
	      }
	    });
	}
```

* Client端随机将请求转发给某个Server节点，获得结果的Future。

```
	public int add(int num1, int num2) throws Exception {
	    long start = System.currentTimeMillis();
	    Map.Entry<String,AddService.ServiceIface> client = getClient();
	    Future<Integer> future = client.getValue().request(num1, num2);
	    try {
	      return Await.result(future);
	    } catch (Exception e) {
	      if (future != null) {
	        future.cancel();
	      }
	      throw e;
	    } finally {
	      long elapse = System.currentTimeMillis() - start;
	      if (elapse > 500) {
	        LOG.warn("Slow add request to {}, cost: {}ms", client.getKey(), elapse);
	      }
	    }
	}

```  
### 要点：
1. **Thrift**：Facebook实现的一种高效的、支持多种编程语言的远程服务调用的框架，通过定义IDL文件可自动生成 RPC客户端与服务端通信相关的代码;
	
2. **Scrooge**：一个Thrift代码解析/生成器，能够生成Scala和Java代码。可以取代Thrift代码生成器，并能在libthrift上生成符合标准的可兼容的二进制编解码;
	
3. **Finagle**：Twitter基于Netty开发的支持容错的、协议无关的RPC框架。其中的网络通信部分基于netty，序列化/反序列化由libthrift提供。libthrift是Apache官方提供的Thrift协议的Java基础模块，提供了底层的网络通讯(基于Java NIO)，Thrift对象序列化与反序列化功能。