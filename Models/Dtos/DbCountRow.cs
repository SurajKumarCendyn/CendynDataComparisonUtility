namespace CendynDataComparisonUtility.Models.Dtos
{
    public class DbCountRow
    {
        public string CendynPropertyId { get; set; }
        public string MongoHotelId { get; set; }
        public string Range { get; set; }
        public string TableName { get; set; }
        public int Count { get; set; }
    }
}
