using CendynDataComparisonUtility.Models.ClientDb; 
using CendynDataComparisonUtility.Models.Dtos;
using CendynDataComparisonUtility.Utility;
using Dapper;
using Microsoft.Data.SqlClient;
using System.Text;

namespace CendynDataComparisonUtility.Data
{
    public class EInDbRepository
    {
        private readonly string _connectionString;
        public EInDbRepository(string connectionString) => _connectionString = connectionString;

        /// <summary>
        /// eIn Customers
        /// </summary>
        /// <returns></returns>
        public IEnumerable<Customer> GetCustomers(bool top100OrRandom)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.Customer); 
            if (top100OrRandom) 
                queryBuilder.Append(" ORDER BY DC.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"); 
            else 
                queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"); 
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<Customer>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// eIn Reservations
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerStay> GetReservations(bool top100OrRandom=true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.Stay);
            if (top100OrRandom)
                queryBuilder.Append(" ORDER BY R.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            else
                queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerStay>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// eIn Stay Details
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerStayDetail> GetStayDetails(bool top100OrRandom=true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.StayDetail);
            if (top100OrRandom)
                queryBuilder.Append(" ORDER BY SR.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            else
                queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerStayDetail>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// Get eIn Transactions
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerTransactions> GetTransactions(bool top100OrRandom=true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.Transactions);
            if (top100OrRandom)
                queryBuilder.Append(" ORDER BY T.TransactionDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            else
                queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerTransactions>(queryBuilder.ToString());
            return result.ToList();
        }

        public List<DbCountRow> GetEInDbCountRows(string cendynPropertyId = null)
        {
            var sql = @"
        SELECT CendynPropertyID AS CendynPropertyId, 'Last 3 years' AS Range, 'Profiles' AS TableName, COUNT(1) AS Count
        FROM D_Customer
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyID = @CendynPropertyId)
          AND DateInserted >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CendynPropertyID
        UNION ALL
        SELECT CendynPropertyID, 'All time', 'Profiles', COUNT(1)
        FROM D_Customer
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyID = @CendynPropertyId)
        GROUP BY CendynPropertyID
        UNION ALL
        SELECT CendynPropertyID, 'Last 3 years', 'Reservations', COUNT(1)
        FROM D_CUSTOMER_STAY
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyID = @CendynPropertyId)
          AND ResCreationDate >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CendynPropertyID
        UNION ALL
        SELECT CendynPropertyID, 'All time', 'Reservations', COUNT(1)
        FROM D_CUSTOMER_STAY
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyID = @CendynPropertyId)
        GROUP BY CendynPropertyID
        UNION ALL
        SELECT CS.CendynPropertyID, 'Last 3 years', 'StayDetail', COUNT(1)
        FROM D_CUSTOMER_STAY_RATE SR
        INNER JOIN D_CUSTOMER_STAY CS ON CS.SourceStayId = SR.SourceStayId
        WHERE (@CendynPropertyId IS NULL OR CS.CendynPropertyID = @CendynPropertyId)
          AND SR.DateInserted >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CS.CendynPropertyID
        UNION ALL
        SELECT CS.CendynPropertyID, 'All time', 'StayDetail', COUNT(1)
        FROM D_CUSTOMER_STAY_RATE SR
        INNER JOIN D_CUSTOMER_STAY CS ON CS.SourceStayId = SR.SourceStayId
        WHERE (@CendynPropertyId IS NULL OR CS.CendynPropertyID = @CendynPropertyId)
        GROUP BY CS.CendynPropertyID
        UNION ALL
        SELECT T.CendynPropertyId, 'Last 3 years', 'Transactions', COUNT(1)
        FROM D_CUSTOMER_STAY_TRANSACTIONS T
        WHERE (@CendynPropertyId IS NULL OR T.CendynPropertyId = @CendynPropertyId)
          AND T.TransactionDate >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY T.CendynPropertyId
        UNION ALL
        SELECT T.CendynPropertyId, 'All time', 'Transactions', COUNT(1)
        FROM D_CUSTOMER_STAY_TRANSACTIONS T
        WHERE (@CendynPropertyId IS NULL OR T.CendynPropertyId = @CendynPropertyId)
        GROUP BY T.CendynPropertyId
    ";

            using var connection = new SqlConnection(_connectionString);
            return connection.Query<DbCountRow>(sql, new { CendynPropertyId = cendynPropertyId }).ToList();
        }

    }
}