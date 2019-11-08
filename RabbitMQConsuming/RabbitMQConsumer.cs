using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQConsuming
{
    public static class RabbitMQConsumer
    {
        /// <summary>
        /// 连接配置
        /// </summary>
        private static readonly ConnectionFactory rabbitMqFactory = new ConnectionFactory()
        {
            HostName = "127.0.0.1",
            UserName = "sc",
            Password = "123456",
            //Port = 5672,
            //VirtualHost = "xyzVirtualHost"
        };
        /// <summary>
        /// 路由名称
        /// </summary>
        const string DirectExchangeName = "routingKey.direct.exchange";

        /// <summary>
        /// 队列名称
        /// </summary>
        const string DirectQueueName = "routingKey.direct.queue";

        /// <summary>
        /// 路由名称
        /// </summary>
        const string TopicExchangeName = "routingKey.topic.exchange";

        /// <summary>
        /// 队列名称
        /// </summary>
        const string TopicQueueName = "routingKey.topic.queue";


        /// <summary>
        /// 基于时间轮询的，每隔一段时间获取一次
        /// </summary>
        public static void DirectAcceptExchange()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(DirectExchangeName, ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(DirectQueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    channel.QueueBind(DirectQueueName, DirectExchangeName, routingKey: DirectQueueName);
                    while (true)
                    {
                        BasicGetResult msgResponse = channel.BasicGet(DirectQueueName, true);
                        if (msgResponse != null)
                        {
                            var msgBody = Encoding.UTF8.GetString(msgResponse.Body);//接收到的消息
                            Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                        }

                        System.Threading.Thread.Sleep(TimeSpan.FromSeconds(1));
                    }
                }
            }
        }
        /// <summary>
        /// 基于事件的，当消息到达时触发事件，获取数据
        /// </summary>
        public static void DirectAcceptExchangeEvent()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    //channel.ExchangeDeclare(ExchangeName, "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(DirectQueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    //channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);
                    //定义这个队列的消费者
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var msgBody = Encoding.UTF8.GetString(ea.Body);//接收到的消息
                        Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                    };
                    //消费者开启监听 autoAck等于false为手动应答，true为自动应答
                    channel.BasicConsume(DirectQueueName, true, consumer: consumer);
                    Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }
        /// <summary>
        /// 基于事件的，当消息到达时触发事件，获取数据
        /// </summary>
        public static void DirectAcceptExchangeTask()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    //channel.ExchangeDeclare(ExchangeName, "direct", durable: true, autoDelete: false, arguments: null);
                    channel.QueueDeclare(DirectQueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    //设置perfetchCount=1 。这样就告诉RabbitMQ 不要在同一时间给一个工作者发送多于1个的消息，或者换句话说。在一个工作者还在处理消息，并且没有响应消息之前，不要给他分发新的消息
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);//告诉broker同一时间只处理一个消息
                    //channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var msgBody = Encoding.UTF8.GetString(ea.Body);//接收到的消息
                        Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                        int dots = msgBody.Split('.').Length - 1;
                        System.Threading.Thread.Sleep(dots * 1000);
                        //返回消息确认 处理完成，告诉Broker可以服务端可以删除消息，分配新的消息过来
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };
                    //消费者开启监听 autoAck设置false,告诉broker，发送消息之后，消息暂时不要删除，等消费者处理完成再说
                    channel.BasicConsume(DirectQueueName, false, consumer: consumer);

                    Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }
        /// <summary>
        /// topic 模糊匹配模式，符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。因此“log.#”能够匹配到“log.info.oa”，但是“log.*” 只会匹配到“log.error”
        /// </summary>
        public static void TopicAcceptExchange()
        {
            using (IConnection conn = rabbitMqFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(TopicExchangeName, ExchangeType.Topic, durable: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(TopicQueueName, durable: false, autoDelete: false, exclusive: false, arguments: null);
                    channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
                    channel.QueueBind(TopicQueueName, TopicExchangeName, routingKey: TopicQueueName);
                    //创建消费者对象
                    var consumer = new EventingBasicConsumer(channel);
                    consumer.Received += (model, ea) =>
                    {
                        var msgBody = Encoding.UTF8.GetString(ea.Body);
                        Console.WriteLine(string.Format("***接收时间:{0}，消息内容：{1}", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss"), msgBody));
                        int dots = msgBody.Split('.').Length - 1;
                        System.Threading.Thread.Sleep(dots * 1000);
                        Console.WriteLine(" [x] Done");
                        channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
                    };
                    //消费者开启监听  autoAck等于false为手动应答，true为自动应答
                    channel.BasicConsume(TopicQueueName, false, consumer: consumer);

                    Console.WriteLine("按任意值，退出程序");
                    Console.ReadKey();
                }
            }
        }
    }
}
