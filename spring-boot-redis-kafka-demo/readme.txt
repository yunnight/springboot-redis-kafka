这个项目和spring-boot-kafka-demo一套

效果：调用spring-boot-kafka-demo的"/producer"接口发送一段消息，spring-boot-kafka-demo会生产到kafka，spring-boot-redis-kafka-demo消费这个消息，按照notify.interval复制4份存入redis，然后ProducerThread扫描redis，到达通知时间的消息取出生产到kafka，再由spring-boot-kafka-demo消费。

首先，要启动zookeeper,redis,kafka