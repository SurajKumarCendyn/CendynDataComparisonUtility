namespace CendynDataComparisonUtility.Models.CenResDb
{
    public class CenResReservations
    {
        public Guid PK_Reservations { get; set; }
        public string ReservationNumber { get; set; }
        public string ExternalResID1 { get; set; } //ReservationNumber
        public string ExternalResID2 { get; set; }
        public string TransactionCode { get; set; }
        public string CendynPropertyID { get; set; }
        public string SubReservationNumber { get; set; }
        public string CentralReservation { get; set; }
        public string BookingEngConfNum { get; set; }
        public string StayStatus { get; set; }
        public DateTime? ArrivalDate { get; set; }
        public DateTime? DepartureDate { get; set; }
        public DateTime? BookingDate { get; set; }
        public DateTime? CancelDate { get; set; }
        public string GroupReservation { get; set; }
        public string Channel { get; set; }
        public string SourceOfBusiness { get; set; }
        public string MarketSeg { get; set; }
        public string MarketSubSeg { get; set; }
        public int? RoomNights { get; set; }
        public int? NumAdults { get; set; }
        public int? NumChildren { get; set; }
        public int? NumYouths { get; set; }
        public int TotalPersons => (NumAdults ?? 0) + (NumYouths ?? 0) + (NumChildren ?? 0);
        public string RateType { get; set; }
        public string RoomTypeCode { get; set; }
        public string RoomCode { get; set; }
        public string IATA { get; set; }
        public int? NumRooms { get; set; }
        public decimal? RoomRevenue { get; set; }
        public decimal? Tax { get; set; }
        public decimal? OtherRevenue { get; set; }
        public decimal? TotalRevenue { get; set; }
    }
}