using Confluent.Kafka;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using ProducerAPI.Models;

namespace ProducerAPI.Controllers
{
    [Route("api/[controller]")]
    [ApiController]
    public class OrderController : ControllerBase
    {
        private readonly IConfiguration _configuration;
        public OrderController(IConfiguration config) 
        {
            _configuration = config;
        }
        [HttpPost]
        public async Task<IActionResult> Post(Order order)
        {
            string message = JsonConvert.SerializeObject(order);
            ProducerConfig config = new ProducerConfig()
            {
                BootstrapServers = _configuration["Kafka:Server"]
            };
            using (var producer = new ProducerBuilder<Null, string>(config).Build())
            {
                var result = await producer.ProduceAsync("test", new Message<Null, string>
                {
                    Value = message
                });
                return await Task.FromResult(Ok("Message Sent"));
            };
        }
    }
}
