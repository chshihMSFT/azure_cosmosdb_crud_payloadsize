
using Microsoft.Azure.Cosmos;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;

namespace azure_cosmosdb_crud_payloadsize
{
    class Program
    {
        static String cosmosdb_uri;
        static String cosmosdb_accountkey;
        static String cosmosdb_dbname;
        static String cosmosdb_containername;
        static String fmt = "0000.00";
        static int sleepTimer = 200;    //1000 = 1 sec, adding latency to avoid throttling in following tests.
        static async Task Main(string[] args)
        {
            IConfigurationRoot configuration = new ConfigurationBuilder()
                        .AddJsonFile("appSettings.json")
                        .Build();
            cosmosdb_uri = configuration["cosmosdb_uri"].ToString();
            cosmosdb_accountkey = configuration["cosmosdb_accountkey"].ToString();
            cosmosdb_dbname = configuration["cosmosdb_dbname"].ToString();
            cosmosdb_containername = configuration["cosmosdb_containername"].ToString();

            CosmosClientOptions clientOptions = new CosmosClientOptions()
            {
                ConnectionMode = ConnectionMode.Direct,
                ApplicationName = String.Format($"20240104_CRUDTest_dotNET7")
            };

            CosmosClient keyClient = new CosmosClient(cosmosdb_uri, cosmosdb_accountkey, clientOptions);
            Database database = keyClient.GetDatabase(cosmosdb_dbname);
            Container container = keyClient.GetContainer(cosmosdb_dbname, cosmosdb_containername);

            try
            {
                Item demodoc = new Item();
                int i = 1;
                while (true)
                {
                    demodoc.id = Guid.NewGuid().ToString();
                    demodoc.pk = "demo";
                    demodoc.counter = i;
                    demodoc.large_payload = String.Concat(Enumerable.Repeat("0123456789", 1)); //100: 1K, 1000: 10K, 10000: 100K
                    demodoc.timestamp = DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff");

                    String demo = JsonConvert.SerializeObject(demodoc);
                    Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, document size = {(demo.Length/1024.0).ToString(fmt)} KB ({demo.Length} bytes)");

                    await CreateItem(i, container, demodoc);
                    await UpsertItem(i, container, demodoc);
                    await ReadItem(i, container, demodoc);
                    await QueryItem(i, container, demodoc);                    
                    await DeleteItem(i, container, demodoc);

                    i++;
                }
            }
            catch (CosmosException ce)
            {
                Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, RBAC Item testing ... Error: {ce.Message}");
            }

        }
        private static async Task CreateItem(int i, Container container, Item demodoc)
        {
            ItemResponse<Item> responseCreate = await container.CreateItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
            Item pointCreateResult = responseCreate.Resource;

            //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateItem, Consumed:{responseCreate.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseCreate.ActivityId}, {{id: {pointCreateResult.id}, pk: {pointCreateResult.pk}, counter: {pointCreateResult.counter}, _ts: {pointCreateResult._ts}, _etag: {pointCreateResult._etag}}}");
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, CreateItem, {responseCreate.RequestCharge.ToString(fmt)} RUs");

            Thread.Sleep(sleepTimer);
        }

        private static async Task ReadItem(int i, Container container, Item demodoc)
        {
            ItemResponse<Item> responseRead = await container.ReadItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));
            Item pointReadResult = responseRead.Resource;

            //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadItem,   Consumed:{responseRead.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseRead.ActivityId}, {{id: {pointReadResult.id}, pk: {pointReadResult.pk}, counter: {pointReadResult.counter}, _ts: {pointReadResult._ts}, _etag: {pointReadResult._etag}}}");
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, ReadItem,   {responseRead.RequestCharge.ToString(fmt)} RUs");
            Thread.Sleep(sleepTimer);
        }

        private static async Task UpsertItem(int i, Container container, Item demodoc)
        {
            demodoc.counter = -1;
            ItemResponse<Item> responseUpsert = await container.UpsertItemAsync<Item>(demodoc, new PartitionKey(demodoc.pk));
            Item pointUpsertResult = responseUpsert.Resource;

            //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, UpsertItem, Consumed:{responseUpsert.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseUpsert.ActivityId}, {{id: {pointUpsertResult.id}, pk: {pointUpsertResult.pk}, counter: {pointUpsertResult.counter}, _ts: {pointUpsertResult._ts}, _etag: {pointUpsertResult._etag}}}");
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, UpsertItem, {responseUpsert.RequestCharge.ToString(fmt)} RUs");
            Thread.Sleep(sleepTimer);
        }
        private static async Task QueryItem(int i, Container container, Item demodoc)
        {
            QueryDefinition query = new QueryDefinition(
                "SELECT TOP 1 * FROM c where c.pk = '" + demodoc.pk + "' and c.id = '" + demodoc.id + "'"
                );
            using (FeedIterator<Item> queryResultSet = container.GetItemQueryIterator<Item>(
                query,
                requestOptions: new QueryRequestOptions()
                {
                    ConsistencyLevel = ConsistencyLevel.Session,
                    MaxItemCount = -1
                }
            ))
            {
                //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItemTEXT,");
                while (queryResultSet.HasMoreResults)
                {
                    int count = 0;
                    FeedResponse<Item> responseMessage = await queryResultSet.ReadNextAsync();
                    foreach (Item doc in responseMessage.Resource)
                    {
                        //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItem,  Consumed:{responseMessage.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseMessage.ActivityId}, {{id: {demodoc.id}, pk: {demodoc.pk}}}");
                        Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, QueryItem,  {responseMessage.RequestCharge.ToString(fmt)} RUs");
                    }
                }
            }
            Thread.Sleep(sleepTimer);
        }
        private static async Task DeleteItem(int i, Container container, Item demodoc)
        {
            ItemResponse<Item> responseDelete = await container.DeleteItemAsync<Item>(demodoc.id, new PartitionKey(demodoc.pk));

            //Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, DeleteItem, Consumed:{responseDelete.RequestCharge.ToString(fmt)} RUs, ActivityId:{responseDelete.ActivityId}, {{id: {demodoc.id}, pk: {demodoc.pk}}}");
            Console.WriteLine($"{DateTime.UtcNow.ToString("yyyy-MM-dd HH:mm:ss.ffffff")}, round {i.ToString("D8")}, DeleteItem, {responseDelete.RequestCharge.ToString(fmt)} RUs");
            Thread.Sleep(sleepTimer);
        }
    }
}