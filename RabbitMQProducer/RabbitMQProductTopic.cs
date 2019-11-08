using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQProducer
{
    public static class RabbitMQProductTopic
    {
        /// <summary>
        ///通配符模式(topic) 生产者
        /// </summary>
        public static void ProductTopic()
        {

            string[] args = null;
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
                    String exchangeName = "exchange3";
                    //路由名称
                    String routeKey = args[0];
                    //声明交换机   通配符类型为topic
                    channel.ExchangeDeclare(exchange: exchangeName, type: "topic");
                    while (true)
                    {
                        Console.WriteLine("消息内容:");
                        String message = Console.ReadLine();
                        //消息内容
                        byte[] body = Encoding.UTF8.GetBytes(message);
                        //发送消息  发送到路由匹配的消息队列中
                        channel.BasicPublish(exchange: exchangeName, routingKey: routeKey, basicProperties: null, body: body);
                        Console.WriteLine("成功发送消息:" + message);
                    }
                }
            }
        }
    }
}
