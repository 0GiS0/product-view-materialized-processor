using System.Collections.Generic;
using Microsoft.Azure.Documents;
using Newtonsoft.Json;

public class Product
{
    public string Region { get; set; }
    public List<Details> details { get; set; }    
    public static Product FromDocument(Document document)
    {
        return JsonConvert.DeserializeObject<Product>(document.ToString());
    }
}

public class Details
{
    public string ProductId { get; set; }
    public int Quantity { get; set; }
}

public class MovieMaterializedView
{
    [JsonProperty("ProductId")]
    public string ProductId;

    [JsonProperty("Count")]
    public int Count;
}