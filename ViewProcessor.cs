using System;
using System.Net;
using System.Threading.Tasks;
using Microsoft.Azure.Documents;
using Microsoft.Azure.Documents.Client;
using Microsoft.Extensions.Logging;


namespace Company.Function
{
    internal class ViewProcessor
    {
        private DocumentClient client;
        private ILogger log;
        private Uri _collectionUri;


        private string _databaseName = Environment.GetEnvironmentVariable("DatabaseName");
        private string _collectionName = Environment.GetEnvironmentVariable("ViewCollectionName");

        public ViewProcessor(DocumentClient client, ILogger log)
        {
            this.client = client;
            this.log = log;
            _collectionUri = UriFactory.CreateDocumentCollectionUri(_databaseName, _collectionName);
        }

        internal async Task UpdateMovieMaterializedView(Product product)
        {
            foreach (var detail in product.details)
            {                
                var optionsSingle = new RequestOptions() { PartitionKey = new PartitionKey(detail.ProductId) };

                MovieMaterializedView viewSingle = null;

                try
                {             
                    var uriSingle = UriFactory.CreateDocumentUri(_databaseName, _collectionName, detail.ProductId);

                    log.LogInformation($"Materialized view: {uriSingle.ToString()}");

                    viewSingle = await client.ReadDocumentAsync<MovieMaterializedView>(uriSingle, optionsSingle);
                }
                catch (DocumentClientException ex)
                {
                    if (ex.StatusCode != HttpStatusCode.NotFound)
                        throw ex;
                }                

                if (viewSingle == null)
                {
                    log.LogInformation("Creating new materialized view");
                    viewSingle = new MovieMaterializedView()
                    {                     
                        ProductId = detail.ProductId,
                        Count = detail.Quantity
                    };
                }
                else
                {
                    log.LogInformation("Updating materialized view");                    
                    viewSingle.Count += detail.Quantity;
                }

                await UpsertDocument(viewSingle, optionsSingle);
            }

        }

        private async Task<ResourceResponse<Document>> UpsertDocument(MovieMaterializedView viewSingle, RequestOptions optionsSingle)
        {
            int attempts = 0;

            while (attempts < 3)
            {
                try
                {
                    var result = await client.UpsertDocumentAsync(_collectionUri, viewSingle, optionsSingle);
                    log.LogInformation($"{optionsSingle.PartitionKey} RU Used: {result.RequestCharge:0.0}");
                    return result;
                }
                catch (DocumentClientException de)
                {
                    if (de.StatusCode == HttpStatusCode.TooManyRequests)
                    {
                        log.LogWarning($"Waiting for {de.RetryAfter} msec...");
                        await Task.Delay(de.RetryAfter);
                        attempts += 1;
                    }
                    else
                    {
                        throw;
                    }
                }
            }

            throw new ApplicationException("Could not insert document after being throttled 3 times");
        }
    }
}