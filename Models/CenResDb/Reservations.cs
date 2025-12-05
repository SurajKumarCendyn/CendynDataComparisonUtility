namespace CendynDataComparisonUtility.Models.CenResDb
{
    public class Reservations
    {
        public Guid Pk_Reservations { get; set; }
        public string? SourceStayID { get; set; }
        public string? CustomerID { get; set; }
        public string? SourceGuestId { get; set; }
        public string? ReservationNumber { get; set; }
        public string? CendynPropertyID { get; set; }
        public string? SubReservationNumber { get; set; }
        public string? CentralReservation { get; set; }
        public string? BookingEngConfNum { get; set; }
        public string? StayStatus { get; set; }
        public DateTime? ArrivalDate { get; set; }
        public DateTime? DepartureDate { get; set; }
        public DateTime? BookingDate { get; set; } // ResCreationDate AS BookingDate
        public DateTime? CancelDate { get; set; }
        public string? GroupReservation { get; set; }
        public string? Channel { get; set; }
        public string? SourceOfBusiness { get; set; }
        public string? MarketSeg { get; set; }
        public string? MarketSubSeg { get; set; }
        public int? RoomNights { get; set; }
        public int? NumAdults { get; set; }
        public int? NumChildren { get; set; }
        public int? TotalPersons { get; set; }
        public string? RateType { get; set; }
        public string? RoomTypeCode { get; set; }
        public string? RoomCode { get; set; }
        public string? IATA { get; set; }
        public int? NumRooms { get; set; }
        public decimal? RoomRevenue { get; set; }
        public decimal? Tax { get; set; }
        public decimal? OtherRevenue { get; set; }
        public decimal? TotalRevenue { get; set; }
        public string? ExternalResID2 { get; set; }
    }
}
