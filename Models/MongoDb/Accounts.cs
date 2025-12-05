using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace CendynDataComparisonUtility.Models.MongoDb
{
    [BsonIgnoreExtraElements]
    public class Accounts
    {
        [BsonId]                    
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = default!;

        [BsonElement("company_name")]
        public string CompanyName { get; set; } = default!;

    }
}
