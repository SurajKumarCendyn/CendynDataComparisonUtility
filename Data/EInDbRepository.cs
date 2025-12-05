using CendynDataComparisonUtility.Models.ClientDb;
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
        public IEnumerable<Customer> GetCustomers()
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.Customer);
            //queryBuilder.Append(" WHERE DC.PK_PROFILES IN ('A942E735-3050-EF11-9E46-0050568A9C71','AA42E735-3050-EF11-9E46-0050568A9C71','E1375616-0E54-EF11-9E46-0050568A9C71','E2375616-0E54-EF11-9E46-0050568A9C71')");
            queryBuilder.Append(" ORDER BY DC.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<Customer>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// eIn Reservations
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerStay> GetReservations()
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.Stay);
            //queryBuilder.Append(" WHERE R.PK_RESERVATIONS IN ('81356166-A660-EF11-9E46-0050568A9C71')");
            queryBuilder.Append(" ORDER BY R.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerStay>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// eIn Stay Details
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerStayDetail> GetStayDetails()
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.EInDb.StayDetail);
            //queryBuilder.Append(" WHERE SR.PK_STAYDETAIL IN ('F31C361E-E5B1-4BE2-B608-0256DCF5DAEA')");
            queryBuilder.Append(" ORDER BY SR.InsertDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerStayDetail>(queryBuilder.ToString());
            return result.ToList();
        }

        /// <summary>
        /// Get eIn Transactions
        /// </summary>
        /// <returns></returns>
        public IEnumerable<CustomerTransactions> GetTransactions()
        {
            var queryBuilder = new StringBuilder( QueryDefinitions.EInDb.Transactions);
            queryBuilder.Append(" ORDER BY T.TransactionDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            using var connection = new SqlConnection(_connectionString);
            var result = connection.Query<CustomerTransactions>(queryBuilder.ToString());
            return result.ToList();
        }
    }
}