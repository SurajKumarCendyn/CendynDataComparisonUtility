using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace CendynDataComparisonUtility.Models.MongoDb
{
    [BsonIgnoreExtraElements]
    public class Transactions
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = default!;

        [BsonElement("account_id")]
        public Object AccountId { get; internal set; }

        [BsonElement("transaction_id")]
        public string TransactionId { get; set; }

        [BsonElement("external_res_id1")]
        public string ExternalResId1 { get; set; } = default!;

        [BsonElement("cendyn_property_id")]
        public string CendynPropertyId { get; set; } = default!;

        [BsonElement("transaction_source")]
        public string TransactionSource { get; set; } = default!;

        [BsonElement("transaction_date")]
        public DateTime TransactionDate { get; set; }

        [BsonElement("transaction_code")]
        public string TransactionCode { get; set; } = default!;

        [BsonElement("currency_code")]
        public string CurrencyCode { get; set; } = default!;

        [BsonElement("credit_amount")]
        public decimal CreditAmount { get; set; }

        [BsonElement("debit_amount")]
        public decimal DebitAmount { get; set; }

        //Ref key
        public Guid Pk_Transactions { get; set; }
    }
}