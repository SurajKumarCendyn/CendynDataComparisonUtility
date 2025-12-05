namespace CendynDataComparisonUtility.Models.ClientDb
{
    public class CustomerTransactions
    {
        public Guid PK_Transactions { get; set; }
        public string ExternalResID1 { get; set; }
        public string ExternalProfileID { get; set; }
        public int CendynPropertyId { get; set; }
        public string TransactionSource { get; set; }
        public string TransactionGroup { get; set; }
        public DateTime TransactionDate { get; set; }
        public string TransactionCode { get; set; }
        public string CurrencyCode { get; set; }
        public decimal CreditAmount { get; set; }
        public decimal DebitAmount { get; set; }
    }
}
