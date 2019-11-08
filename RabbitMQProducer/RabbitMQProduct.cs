using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQProducer
{
    public static class RabbitMQProduct
    {
        /// <summary>
        /// 连接配置
        /// </summary>
        private static readonly ConnectionFactory rabbitMQFactory = new ConnectionFactory()//创建连接工厂对象
        {
            HostName = "127.0.0.1",
            UserName = "sc",
            Password = "123456",
            //Port = 5672,//端口号
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
        ///  Direct单点精确路由模式
        /// </summary>
        public static void DirectExchangeSendMsg()
        {
            using (IConnection conn = rabbitMQFactory.CreateConnection())//创建连接对象
            {
                using (IModel channel = conn.CreateModel())//创建连接会话对象
                {
                    //设置交换器的类型
                    channel.ExchangeDeclare(DirectExchangeName, ExchangeType.Direct, durable: true, autoDelete: false, arguments: null);
                    //声明一个队列，设置队列是durable否持久化，排他性，与自动删除
                    channel.QueueDeclare(DirectQueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
                    //绑定消息队列，交换器，routingkey
                    channel.QueueBind(DirectQueueName, DirectExchangeName, routingKey: DirectQueueName);

                    var props = channel.CreateBasicProperties();
                    //工作队列 投放模式
                    //props.DeliveryMode = 2;
                    //队列消息持久化
                    props.Persistent = true;
                    string vadata = Console.ReadLine();
                    while (vadata != "exit")
                    {
                        var msgBody = Encoding.UTF8.GetBytes(vadata);
                        //发送消息
                        channel.BasicPublish(exchange: DirectExchangeName, routingKey: DirectQueueName, basicProperties: props, body: msgBody);
                        Console.WriteLine(string.Format("***发送时间:{0}，发送完成，输入exit退出消息发送",
                            DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));
                        vadata = Console.ReadLine();
                    }
                }
            }
        }
        /// <summary>
        /// topic 模糊匹配模式，符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。因此“log.#”能够匹配到“log.info.oa”，但是“log.*” 只会匹配到“log.error”
        /// </summary>
        public static void TopicExchangeSendMsg()
        {
            using (IConnection conn = rabbitMQFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    channel.ExchangeDeclare(TopicExchangeName, ExchangeType.Topic, durable: false, autoDelete: false, arguments: null);
                    channel.QueueDeclare(TopicQueueName, durable: false, autoDelete: false, exclusive: false, arguments: null);
                    channel.QueueBind(TopicQueueName, TopicExchangeName, routingKey: TopicQueueName);
                    //var props = channel.CreateBasicProperties();
                    //props.Persistent = true;
                    string vadata = Console.ReadLine();
                    while (vadata != "exit")
                    {
                        var msgBody = Encoding.UTF8.GetBytes(vadata);
                        channel.BasicPublish(exchange: TopicExchangeName, routingKey: TopicQueueName, basicProperties: null, body: msgBody);
                        Console.WriteLine(string.Format("***发送时间:{0}，发送完成，输入exit退出消息发送", DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")));
                        vadata = Console.ReadLine();
                    }
                }
            }
        }
    }
}
