using CendynDataComparisonUtility.Models.CenResNormalize;
using CendynDataComparisonUtility.Utility;
using Dapper;
using Microsoft.Data.SqlClient;

namespace CendynDataComparisonUtility.Service
{
    public class CenResNormalizeDbRepository
    {
        private readonly string _connectionString;
        public CenResNormalizeDbRepository(string connectionString) => _connectionString = connectionString;

        public IEnumerable<CenResNCustomer> GetCustomers(string parentCompanyId, List<string> customerIds)
        {
            var query = QueryDefinitions.CenResNormalizeDb.Customer;
            query += " WHERE C.ParentCompanyId=@ParentCompanyId AND C.CustomerID IN @CustomerIds";
            var parameters = new { ParentCompanyId = parentCompanyId, CustomerIds = customerIds };
            using var connection = new SqlConnection(_connectionString);
            var customers = connection.Query<CenResNCustomer>(query, parameters).ToList();
            return customers;
        }

        public IEnumerable<CenResNReservations> GetReservation(string parentCompanyId, List<string> purchaseIds)
        {
            string query = QueryDefinitions.CenResNormalizeDb.Stays + " WHERE ParentCompanyId=@ParentCompanyId AND StayId IN @PurchaseIds";
            var parameters = new { ParentCompanyId = parentCompanyId, PurchaseIds = purchaseIds };

            using var connection = new SqlConnection(_connectionString);
            var cenResReservations = connection.Query<CenResNReservations>(query, parameters).ToList();
            return cenResReservations;
        }

        //Get Stay Details
        public IEnumerable<CenResNStayDetail> GetStayDetails(string parentCompanyId,  List<string> stayDetailsId)
        {
            string query = QueryDefinitions.CenResNormalizeDb.StayDetail + " WHERE SD.ParentCompanyId=@ParentCompanyId AND SD.StayDetailId IN @StayDetailsId";
            var parameters = new { ParentCompanyId = parentCompanyId, StayDetailsId = stayDetailsId };
            using var connection = new SqlConnection(_connectionString);
            var cenResStayDetails = connection.Query<CenResNStayDetail>(query, parameters).ToList();
            return cenResStayDetails;
        }

        //Get Transactions
        public IEnumerable<CenResNTransactions> GetTransactions(string parentCompanyId , List<MongoTransactionMapForCenResNDb> transactionIds)
        {
            string query = QueryDefinitions.CenResNormalizeDb.Transactions + " WHERE ParentCompanyId=@ParentCompanyId AND StayTransactionsId IN @TransactionIds";
            var parameters = new { ParentCompanyId = parentCompanyId, TransactionIds = transactionIds.Select(t => t.Mongo_TransactionId) };
            using var connection = new SqlConnection(_connectionString);
            var cenResTransactions = connection.Query<CenResNTransactions>(query, parameters).ToList();
            foreach (var transaction in cenResTransactions)
            {
                var mapping = transactionIds.FirstOrDefault(t => t.Mongo_TransactionId == transaction.StayTransactionsID.ToString());
                if (mapping != null)
                {
                    transaction.Pk_Transactions = mapping.Pk_transactions;
                }
            }   
            return cenResTransactions;
        }


        public class MongoTransactionMapForCenResNDb
        {
            public string Mongo_TransactionId { get; set; }

            //Ref
            public Guid Pk_transactions { get; set; }
        }
    }
}
