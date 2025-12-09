using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace CendynDataComparisonUtility.Models.MongoDb
{
    [BsonIgnoreExtraElements]
    public class Contacts
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = default!;

        [BsonElement("user_id")]
        public string UserId { get; set; }

        [BsonElement("account_id")]
        public ObjectId AccountId { get; set; } = default!;

        [BsonElement("cendyn_property_id")]
        public string CendynPropertyId { get; set; } = default!;

        [BsonElement("title")]
        public string Title { get; set; }

        [BsonElement("name_first")]
        public string FirstName { get; set; }

        [BsonElement("name_last")]
        public string LastName { get; set; }

        [BsonElement("salutation")]
        public string Salutation { get; set; }

        [BsonElement("address_1")]
        public string Address1 { get; set; }

        [BsonElement("city")]
        public string City { get; set; }

        [BsonElement("region")]
        public string Region { get; set; }   //StateProvinceCode

        [BsonElement("zip")]
        public string PostalCode { get; set; }


        [BsonElement("phone_countrycode")]
        public string CountryCode { get; set; }

        [BsonElement("phone_number")]
        public string PhoneNumber { get; set; }

        [BsonElement("email")]
        public string Email { get; set; }

        [BsonElement("language")]
        public string Language { get; set; }

        [BsonElement("nationality")]
        public string Nationality { get; set; }

        [BsonElement("company_name")]
        public string CompanyName { get; set; }

        [BsonElement("job_title")]
        public string JobTitle { get; set; }

        [BsonElement("allow_email")]
        public bool AllowEmail { get; set; }

        [BsonElement("allow_mail")]
        public bool AllowMail { get; set; }
        public Guid Pk_Profiles { get; set; }

        [BsonElement("hotel_id")]
        public ObjectId HotelId { get; set; } = default!;
    }
}