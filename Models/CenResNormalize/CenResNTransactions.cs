namespace CendynDataComparisonUtility.Models.CenResNormalize
{
    public class CenResNTransactions
    {
        public string StayTransactionsID  { get; set; }
        public Guid Pk_Transactions { get; set; } 
        public string TransactionSource { get; set; }
        public string TransactionGroup { get; set; }
        public DateTimeOffset TransactionDate { get; set; }
        public string TransactionCode { get; set; }
        public string CurrencyCode { get; set; }
        public string CreditAmount { get; set; }
        public string DebitAmount { get; set; }
    }
}