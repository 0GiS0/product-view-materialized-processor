using System.Collections.Generic;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;

namespace Company.Function
{
    public static class OrderOrderDetails
    {
        [FunctionName("OrderOrderDetails")]
        public async static Task Run([CosmosDBTrigger(
            databaseName: "Movies",
            collectionName: "OrderOrderDetails",
            ConnectionStringSetting = "CosmosDbConnection")]IReadOnlyList<Document> input, ILogger log,
             [CosmosDB(
                databaseName: "Movies",
                collectionName: "ItemCount",
                ConnectionStringSetting = "CosmosDbConnection"
            )]DocumentClient client
            )
        {
            log.LogInformation($"Triggered");

            if (input != null && input.Count > 0)
            {
                var viewProcessor = new ViewProcessor(client, log);

                log.LogInformation($"Processing {input.Count} events");

                foreach (var product in input)
                {
                    var tasks = new List<Task>();

                    tasks.Add(viewProcessor.UpdateMovieMaterializedView(Product.FromDocument(product)));

                    await Task.WhenAll(tasks);
                }
            }

        }
    }
}
