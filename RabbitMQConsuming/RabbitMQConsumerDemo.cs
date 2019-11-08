using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQConsuming
{
    public static class RabbitMQConsumerDemo
    {
        /// <summary>
        /// 连接配置
        /// </summary>
        private static readonly ConnectionFactory factory = new ConnectionFactory()
        {
            HostName = "127.0.0.1",
            UserName = "sc",
            Password = "123456",
            //Port = 5672,
            //VirtualHost = "xyzVirtualHost"
        };

        /// <summary>
        /// 通配符模式(topic) 交换机名称
        /// </summary>
        private const string ExchangeName = "rabbitMQ.common.exchange";

        /// <summary>
        /// 路由名称
        /// </summary>
        private const string RoutingKeyName = "rabbitMQ.common.routingKey";

        /// <summary>
        /// 队列名称
        /// </summary>
        private const string QueueName = "rabbitMQ.common.queue";

        /// <summary>
        /// 消费者接收消息
        /// </summary>
        /// <param name="msg">发送的消息内容</param>
        /// <param name="persistent">是否持久化</param>
        /// <param name="exchangeType">消息模式：发布订阅模式(fanout)；路由模式(direct) ；通配符模式(topic) 生产者 模糊匹配模式，符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。因此“log.#”能够匹配到“log.info.oa”，但是“log.*” 只会匹配到“log.error”</param>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routingKeyName">路由名称</param>
        public static void ConsumerReceive(string msg, bool durable = false,  string exchangeType = ExchangeType.Direct, string exchangeName = ExchangeName, string queueName = QueueName, string routingKeyName = RoutingKeyName)
        {
            using IConnection conn = factory.CreateConnection();
            using IModel channel = conn.CreateModel();
            channel.ExchangeDeclare(ExchangeName, exchangeType, durable: durable, autoDelete: false, arguments: null);
            channel.QueueDeclare(QueueName, durable: false, autoDelete: false, exclusive: false, arguments: null);
            channel.BasicQos(prefetchSize: 0, prefetchCount: 1, global: false);
            channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);
            //创建消费者对象
            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, ea) =>
            {
                var msgBody = Encoding.UTF8.GetString(ea.Body);
                int dots = msgBody.Split('.').Length - 1;
                System.Threading.Thread.Sleep(dots * 1000);
                channel.BasicAck(deliveryTag: ea.DeliveryTag, multiple: false);
            };
            //消费者开启监听  autoAck等于false为手动应答，true为自动应答
            channel.BasicConsume(QueueName, false, consumer: consumer);
        }
    }
}
