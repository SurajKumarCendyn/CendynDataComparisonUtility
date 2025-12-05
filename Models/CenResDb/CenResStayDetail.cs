namespace CendynDataComparisonUtility.Models.CenResDb
{
    public class CenResStayDetail
    {
        public Guid PK_StayDetail { get; set; }
        public Guid FK_Reservations { get; set; }
        public string ReservationNumber { get; set; }
        public string CendynPropertyId { get; set; }
        public string StayDetailID { get; set; }
        public DateTime StayDate { get; set; }
        public string RoomNumber { get; set; }
        public string SourceRoomType { get; set; }
        public string SourceRateType { get; set; }
        public int NumberOfRooms { get; set; }
        public string CurrencyCode { get; set; }
        public decimal DailyRate { get; set; }
        public string MarketCode { get; set; }

        public string StayRateType { get; set; }
        public string StayRoomType { get; set; }
        public decimal StayRateAmount { get; set; }
    }
}