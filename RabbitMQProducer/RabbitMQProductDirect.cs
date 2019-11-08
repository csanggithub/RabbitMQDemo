using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.Text;

namespace RabbitMQProducer
{
    public static class RabbitMQProductDirect
    {
        /// <summary>
        /// 路由模式(direct) 生产者
        /// </summary>
        public static void ConsumerDirect()
        {
            string[] args =null;
            Console.WriteLine("Start");
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
                    String exchangeName = "exchange2";
                    //路由名称
                    String routeKey = args[0];
                    //声明交换机   路由交换机类型direct
                    channel.ExchangeDeclare(exchange: exchangeName, type: "direct");
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
