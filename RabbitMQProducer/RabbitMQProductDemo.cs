using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQProducer
{
    /// <summary>
    /// RabbitMQ 生产者
    /// </summary>
    public static class RabbitMQProductDemo
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
        /// 生产者发送消息
        /// </summary>
        /// <param name="msg">发送的消息内容</param>
        /// <param name="durable">是否持久化</param>
        /// <param name="deliveryMode">是否平均投放 等于2则平均投放</param>
        /// <param name="exchangeType">消息模式：发布订阅模式(fanout)；路由模式(direct) ；通配符模式(topic) 生产者 模糊匹配模式，符号“#”匹配一个或多个词，符号“*”匹配不多不少一个词。因此“log.#”能够匹配到“log.info.oa”，但是“log.*” 只会匹配到“log.error”</param>
        /// <param name="exchangeName">交换机名称</param>
        /// <param name="queueName">队列名称</param>
        /// <param name="routingKeyName">路由名称</param>
        public static void ProductSend(string msg, bool durable = false, byte deliveryMode = 1, string exchangeType = ExchangeType.Direct, string exchangeName = ExchangeName, string queueName = QueueName, string routingKeyName = RoutingKeyName)
        {
            using IConnection conn = factory.CreateConnection();
            using IModel channel = conn.CreateModel();
            //声明交换机 设置交换器的类型
            channel.ExchangeDeclare(ExchangeName, exchangeType, durable: true, autoDelete: false, arguments: null);
            //声明一个队列，设置队列是durable否持久化，排他性，与自动删除
            channel.QueueDeclare(QueueName, durable: true, autoDelete: false, exclusive: false, arguments: null);
            //绑定消息队列，交换器，routingkey
            channel.QueueBind(QueueName, ExchangeName, routingKey: QueueName);

            var props = channel.CreateBasicProperties();
            //工作队列 投放模式
            props.DeliveryMode = deliveryMode;
            //队列消息持久化
            props.Persistent = durable;

            var msgBody = Encoding.UTF8.GetBytes(msg);
            //发送消息
            channel.BasicPublish(exchange: ExchangeName, routingKey: RoutingKeyName, basicProperties: props, body: msgBody);
        }

    }
}
