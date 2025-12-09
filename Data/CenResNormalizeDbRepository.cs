using CendynDataComparisonUtility.Models.CenResNormalize; 
using CendynDataComparisonUtility.Models.Dtos;
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

        public List<DbCountRow> GetCenResNormalizeDbCountRows(List<CendynPropertyMongoHotelIdMapping> propertyIds)
        {
            var sql = @"
        SELECT @CendynPropertyId AS CendynPropertyId, PropertyId AS MongoHotelId, 'Last 3 years' AS Range, 'Profiles' AS TableName, COUNT(1) AS Count
        FROM CCRM.CUSTOMER
        WHERE PropertyID IN @PropertyIds AND DateInserted >= DATEADD(YEAR, -3, GETDATE()) GROUP BY PropertyID
        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, PropertyId AS MongoHotelId, 'All time' AS Range, 'Profiles' AS TableName, COUNT(1) AS Count
        FROM CCRM.CUSTOMER
        WHERE PropertyID IN @PropertyIds GROUP BY PropertyID

        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, PropertyId AS MongoHotelId, 'Last 3 years' AS Range, 'Reservations' AS TableName, COUNT(1) AS Count
        FROM CCRM.Stays
        WHERE PropertyID IN @PropertyIds AND BookingDate >= DATEADD(YEAR, -3, GETDATE()) GROUP BY PropertyID
        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, PropertyId AS MongoHotelId, 'All time' AS Range, 'Reservations' AS TableName, COUNT(1) AS Count
        FROM CCRM.Stays WITH(NOLOCK)
        WHERE PropertyID IN @PropertyIds GROUP BY PropertyID

        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, SD.PropertyId AS MongoHotelId, 'Last 3 years' AS Range, 'StayDetail' AS TableName, COUNT(1) AS Count
        FROM CCRM.StayDetail SD WITH(NOLOCK)
        WHERE SD.PropertyId IN @PropertyIds AND SD.StayDate >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY SD.PropertyId
        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, SD.PropertyId AS MongoHotelId, 'All time' AS Range, 'StayDetail' AS TableName, COUNT(1) AS Count
        FROM CCRM.StayDetail SD WITH(NOLOCK)
        WHERE SD.PropertyId IN @PropertyIds GROUP BY SD.PropertyId

        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, T.PropertyId AS MongoHotelId, 'Last 3 years' AS Range, 'Transactions' AS TableName, COUNT(1) AS Count
        FROM CCRM.StayTransactions T WITH(NOLOCK)
        WHERE T.PropertyId IN @PropertyIds AND T.TransactionDate >= DATEADD(YEAR, -3, GETDATE()) GROUP BY T.PropertyId
        UNION ALL
        SELECT @CendynPropertyId AS CendynPropertyId, T.PropertyId AS MongoHotelId, 'All time' AS Range, 'Transactions' AS TableName, COUNT(1) AS Count
        FROM CCRM.StayTransactions T WITH(NOLOCK)
        WHERE T.PropertyId IN @PropertyIds GROUP BY T.PropertyId";

            using var connection = new SqlConnection(_connectionString);
            var cendynPropertyId = propertyIds.FirstOrDefault()?.CendynPropertyId ?? string.Empty;
            var mongoPropertyIds = propertyIds.Select(x => x.MongoPropertyId).ToList();
            return connection.Query<DbCountRow>(sql, new { CendynPropertyId = cendynPropertyId, PropertyIds = mongoPropertyIds }).ToList();
        }
        public class MongoTransactionMapForCenResNDb
        {
            public string Mongo_TransactionId { get; set; }

            //Ref
            public Guid Pk_transactions { get; set; }
        }

        
    }
}
