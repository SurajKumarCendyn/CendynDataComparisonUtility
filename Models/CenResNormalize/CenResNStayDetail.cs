namespace CendynDataComparisonUtility.Models.CenResNormalize
{
    public class CenResNStayDetail
    {
        public string ReservationNumber { get; set; }
        public string CendynPropertyId { get; set; }

        public DateTimeOffset StayDate { get; set; }     
        public string StayRateType { get; set; }
        public string StayRoomType { get; set; }
        public decimal StayRateAmount { get; set; }
        public string StayNumRooms { get; set; }
        public string CurrencyCode { get; set; }
    }
}