using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace CendynDataComparisonUtility.Models.MongoDb
{
    [BsonIgnoreExtraElements]
    public class Purchases
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = default!;

        [BsonElement("account_id")]
        public Object AccountId { get; set; } = default!;

        [BsonElement("stay_daily_rate")]
        public decimal StayDailyRate { get; set; } = default!;
  
        [BsonElement("price")]
        public decimal Price { get; set; } = default!;

        [BsonElement("check_in")]
        public DateTime? CheckIn { get; set; } = default!;

        [BsonElement("check_out")]
        public DateTime? CheckOut { get; set; } = default!;

        [BsonElement("number_rooms")]
        public int NumberRooms { get; set; } = default!;

        [BsonElement("room_revenue")]
        public decimal RoomRevenue { get; set; } = default!;

        [BsonElement("total_tax")]
        public decimal TotalTax { get; set; }

        [BsonElement("total_other_revenue")]
        public decimal TotalOtherRevenue { get; set; } = default!;

        [BsonElement("total_revenue")]
        public decimal TotalRevenue { get; set; } = default!;        

        [BsonElement("transaction_code")]
        public string TransactionCode { get; set; } = default!;

        [BsonElement("confirmation_number")]
        public string ConfirmationNumber { get; set; } = default!;

        [BsonElement("central_res_num")]
        public string CentralResNum { get; set; } = default!;

        [BsonElement("booking_source_name")]
        public string BookingSourceName { get; set; } = default!;

        [BsonElement("res_status_code")]
        public string ResStatusCode { get; set; } = default!;

        [BsonElement("res_arrive_time")]
        public DateTime? ResArriveDate { get; set; } = default!;

        [BsonElement("res_depart_time")]
        public DateTime? ResDepartDate { get; set; } = default!;

        [BsonElement("booking_date")]
        public DateTime? BookingDate { get; set; } = default!;

        [BsonElement("cancellation_date")]
        public DateTime? CancelDate { get; set; } = default!;

        [BsonElement("group_name")]
        public string GroupName { get; set; } = default!;

        [BsonElement("stay_channel_code")]
        public string StayChannelCode { get; set; } = default!;

        [BsonElement("market_segment_code")]
        public string MarketSegmentCode { get; set; } = default!;

        [BsonElement("num_of_nights")]
        public int NumOfNights { get; set; } = default!;

        [BsonElement("number_of_adults")]
        public int NumOfAdults { get; set; } = default!;

        [BsonElement("number_of_childs")]
        public int NumOfChildren { get; set; } = default!;

        [BsonElement("total_persons")]
        public int TotalPersons { get; set; }

        [BsonElement("stay_rate_type")]
        public string RateType { get; set; }

        [BsonElement("room_type_code")]
        public string RoomTypeCode { get; set; }

        [BsonElement("room_code")]
        public string RoomCode { get; set; }

        [BsonElement("trvl_agnt_iata")]
        public string TravelAgentIata { get; set; } = default!;

        //keys to map with
        [BsonElement("uniq_id")]
        public string UniqId_ExternalResID1 { get; set; } = default!;  // ReservationNumber in CenRes

        [BsonElement("UUID")]
        public string Uuid_CendynPropertyID { get; set; } = default!;

        [BsonElement("purchase_stay_details")]
        public List<StayDetail> PurchaseStayDetails { get; set; } = default!;
    }
    
    [BsonIgnoreExtraElements]
    public class StayDetail
    {
        [BsonId]
        [BsonRepresentation(BsonType.ObjectId)]
        public string Id { get; set; } = default!;

        [BsonElement("stay_detail_stay_date")]
        public DateTime? StayDate { get; set; } = default!;

        [BsonElement("stay_detail_rate_type")]
        public string RateType { get; set; } = default!;

        [BsonElement("stay_detail_room_type")]
        public string RoomType { get; set; } = default!; 

        [BsonElement("stay_rate_amount_corp")]
        public decimal StayRateAmount { get; set; } = default!;

        [BsonElement("stay_detail_currency_code")]
        public string CurrencyCode { get; set; } = default!;

        [BsonElement("stay_detail_room_number")]
        public int? NumberOfRooms { get; set; } = default!;

        public string UniqId_ExternalResID1 { get; set; } = default!;   

        public string Uuid_CendynPropertyID { get; set; } = default!;
    }
}