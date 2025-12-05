namespace CendynDataComparisonUtility.Models.ClientDb
{
    public class CustomerStayDetail
    {
        public Guid Pk_StayDetail { get; set; }
        public int RateId { get; set; }
        public int SourceStayId { get; set; }
        public int CustomerId { get; set; }
        public string ReservationNumber { get; set; }
        public int CendynPropertyID { get; set; }
        public DateTime StayDate { get; set; }
        public string StayRateType { get; set; }
        public string StayRoomType { get; set; }
        public decimal StayRateAmount { get; set; }
        public int StayNumRooms { get; set; }
        public string CurrencyCode { get; set; }
    }
}
