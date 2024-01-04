namespace azure_cosmosdb_crud_payloadsize
{
    // <Model>
    public class Item
    {
        public string id { get; set; }
        public string pk { get; set; }
        public int counter { get; set; }
        public string timestamp { get; set; }
        public string large_payload { get; set; }
    }
    // </Model>
}
