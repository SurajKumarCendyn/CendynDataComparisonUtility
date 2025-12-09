using CendynDataComparisonUtility.Models.CenResDb;
using CendynDataComparisonUtility.Utility;
using Dapper;
using Microsoft.Data.SqlClient;
using System.Text;

namespace CendynDataComparisonUtility.Service
{
    public class CenResDbRepository
    {
        private readonly string _connectionString;
        public CenResDbRepository(string connectionString) => _connectionString = connectionString;

        public IEnumerable<CenResProfiles> GetProfiles(List<Guid> pk_profileIds = null, int feature = 1)
        {
            var queryBuilder = new StringBuilder( QueryDefinitions.CenResDb.Profiles);
            if (feature == 1 && pk_profileIds != null && pk_profileIds.Count > 0)
            {
                queryBuilder.Append(" WHERE P.PK_Profiles IN @Ids"); //({string.Join(",", pk_profileIds.Select(id => $"'{id}'"))})
                queryBuilder.Append (@" GROUP BY
                        P.PK_Profiles,
                        P.Salutation,
                        P.FirstName,
                        P.LastName,
                        P.ExternalProfileID,
                        P.CendynPropertyId,
                        P.ExternalProfileID2,
                        AD.Address1,
                        AD.City,
                        AD.StateProvince,
                        AD.PostalCode,
                        AD.CountryCode,
                        P.Nationality,
                        P.PrimaryLanguage,
                        P.CompanyName,
                        P.AllowMail,
                        P.AllowEmail,
                        P.JobTitle;");
            }
            else if (feature == 2)
            {
                queryBuilder.Append(" WHERE CendynPropertyId='1054'"); //Hardcode for testing
                queryBuilder.Append(@" GROUP BY
                        P.PK_Profiles,
                        P.Salutation,
                        P.FirstName,
                        P.LastName,
                        P.ExternalProfileID,
                        P.CendynPropertyId,
                        P.ExternalProfileID2,
                        AD.Address1,
                        AD.City,
                        AD.StateProvince,
                        AD.PostalCode,
                        AD.CountryCode,
                        P.Nationality,
                        P.PrimaryLanguage,
                        P.CompanyName,
                        P.AllowMail,
                        P.AllowEmail,
                        P.JobTitle,
                        P.DateInserted");
                queryBuilder.Append(" ORDER BY P.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 10 ROWS ONLY");
            }

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_profileIds != null && pk_profileIds.Count > 0) 
                return connection.Query<CenResProfiles>(queryBuilder.ToString(), new { Ids = pk_profileIds }).ToList(); 
            else
                return connection.Query<CenResProfiles>(queryBuilder.ToString()).ToList();
       }

        public IEnumerable<CenResReservations> GetReservations(List<Guid> pk_reservationIds = null, int feature = 1)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.Reservations);
            if (feature == 1 && pk_reservationIds != null && pk_reservationIds.Count > 0)
                queryBuilder.Append(" WHERE R.PK_Reservations IN @Ids");
            else if (feature == 2)
                queryBuilder.Append(" ORDER BY R.DateResMade DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_reservationIds != null && pk_reservationIds.Count > 0)
                return connection.Query<CenResReservations>(queryBuilder.ToString(), new { Ids = pk_reservationIds }).ToList();
            else
                return connection.Query<CenResReservations>(queryBuilder.ToString()).ToList();
        }

        //Get Stay Details
        public IEnumerable<CenResStayDetail> GetStayDetails(List<Guid> pk_stayDetailIds = null, int feature = 1)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.StayDetail);
            if (feature == 1 && pk_stayDetailIds != null && pk_stayDetailIds.Count > 0)
                queryBuilder.Append(" WHERE SD.PK_StayDetail IN @Ids"); 
            else if (feature == 2)
                queryBuilder.Append(" ORDER BY SD.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_stayDetailIds != null && pk_stayDetailIds.Count > 0)
                return connection.Query<CenResStayDetail>(queryBuilder.ToString(), new { Ids = pk_stayDetailIds });
            else
                return connection.Query<CenResStayDetail>(queryBuilder.ToString());
        }

        public IEnumerable<CenResTransactions> GetTransactions(List<Guid> pk_transactionIds = null, int feature = 1)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.Transactions);
            if (feature == 1 && pk_transactionIds != null && pk_transactionIds.Count > 0)
                queryBuilder.Append(" WHERE T.PK_Transactions IN @Ids");
            else if (feature == 2)
                queryBuilder.Append(" ORDER BY T.TransactionDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_transactionIds != null && pk_transactionIds.Count > 0)
                return connection.Query<CenResTransactions>(queryBuilder.ToString(), new { Ids = pk_transactionIds });
            else
                return connection.Query<CenResTransactions>(queryBuilder.ToString());
        }
    }
}