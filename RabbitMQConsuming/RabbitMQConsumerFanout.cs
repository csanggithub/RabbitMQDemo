using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQConsuming
{
    public static class RabbitMQConsumerFanout
    {
        /// <summary>
        /// 发布订阅模式(fanout) 消费者
        /// </summary>
        public static void ConsumerFanout()
        {
            //创建一个随机数,以创建不同的消息队列
            int random = new Random().Next(1, 1000);
            Console.WriteLine("Start" + random.ToString());
            IConnectionFactory connFactory = new ConnectionFactory//创建连接工厂对象
            {
                HostName = "47.104.206.56",//IP地址
                Port = 5672,//端口号
                UserName = "yan",//用户账号
                Password = "yan"//用户密码
            };
            using (IConnection conn = connFactory.CreateConnection())
            {
                using (IModel channel = conn.CreateModel())
                {
                    //交换机名称
                    String exchangeName = "exchange1";
                    //声明交换机
                    channel.ExchangeDeclare(exchange: exchangeName, type: "fanout");
                    //消息队列名称
                    String queueName = exchangeName + "_" + random.ToString();
                    //声明队列
                    channel.QueueDeclare(queue: queueName, durable: false, exclusive: false, autoDelete: false, arguments: null);
                    //将队列与交换机进行绑定
                    channel.QueueBind(queue: queueName, exchange: exchangeName, routingKey: "");
                    //声明为手动确认
                    channel.BasicQos(0, 1, false);
                    //定义消费者
                    var consumer = new EventingBasicConsumer(channel);
                    //接收事件
                    consumer.Received += (model, ea) =>
                    {
                        byte[] message = ea.Body;//接收到的消息
                        Console.WriteLine("接收到信息为:" + Encoding.UTF8.GetString(message));
                        //返回消息确认
                        channel.BasicAck(ea.DeliveryTag, true);
                    };
                    //开启监听
                    channel.BasicConsume(queue: queueName, autoAck: false, consumer: consumer);
                    Console.ReadKey();
                }
            }
        }
    }
}
