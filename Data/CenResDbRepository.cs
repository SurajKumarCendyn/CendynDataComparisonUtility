using CendynDataComparisonUtility.Models.CenResDb;
using CendynDataComparisonUtility.Models.Dtos;
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

        public IEnumerable<CenResProfiles> GetProfiles(List<Guid> pk_profileIds = null, int feature = 1 , bool top100OrRandom =true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.Profiles);
            if (feature == 1 && pk_profileIds != null && pk_profileIds.Count > 0)
            {
                queryBuilder.Append(" WHERE P.PK_Profiles IN @Ids"); 
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
                        P.AllowMarketResearch,
                        P.JobTitle;");
            }
            else if (feature == 2)
            {
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
           P.AllowMarketResearch,
                        P.DateInserted");
                if (top100OrRandom) 
                    queryBuilder.Append(" ORDER BY P.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"); 
                else 
                    queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"); 

            }

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_profileIds != null && pk_profileIds.Count > 0)
                return connection.Query<CenResProfiles>(queryBuilder.ToString(), new { Ids = pk_profileIds }).ToList();
            else
                return connection.Query<CenResProfiles>(queryBuilder.ToString()).ToList();
        }

        public IEnumerable<CenResReservations> GetReservations(List<Guid> pk_reservationIds = null, int feature = 1 ,bool top100OrRandom=true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.Reservations);
            if (feature == 1 && pk_reservationIds != null && pk_reservationIds.Count > 0)
                queryBuilder.Append(" WHERE R.PK_Reservations IN @Ids");
            else if (feature == 2)
            {
                if (top100OrRandom) 
                    queryBuilder.Append(" ORDER BY R.DateResMade DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY"); 
                else 
                    queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            } 

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_reservationIds != null && pk_reservationIds.Count > 0)
                return connection.Query<CenResReservations>(queryBuilder.ToString(), new { Ids = pk_reservationIds }).ToList();
            else
                return connection.Query<CenResReservations>(queryBuilder.ToString()).ToList();
        }

        //Get Stay Details
        public IEnumerable<CenResStayDetail> GetStayDetails(List<Guid> pk_stayDetailIds = null, int feature = 1, bool top100OrRandom = true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.StayDetail);
            if (feature == 1 && pk_stayDetailIds != null && pk_stayDetailIds.Count > 0)
                queryBuilder.Append(" WHERE SD.PK_StayDetail IN @Ids");
            else if (feature == 2)
            {
                if (top100OrRandom)
                    queryBuilder.Append(" ORDER BY SD.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
                else
                    queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            } 

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_stayDetailIds != null && pk_stayDetailIds.Count > 0)
                return connection.Query<CenResStayDetail>(queryBuilder.ToString(), new { Ids = pk_stayDetailIds });
            else
                return connection.Query<CenResStayDetail>(queryBuilder.ToString());
        }

        public IEnumerable<CenResTransactions> GetTransactions(List<Guid> pk_transactionIds = null, int feature = 1 , bool top100OrRandom = true)
        {
            var queryBuilder = new StringBuilder(QueryDefinitions.CenResDb.Transactions);
            if (feature == 1 && pk_transactionIds != null && pk_transactionIds.Count > 0)
                queryBuilder.Append(" WHERE T.PK_Transactions IN @Ids");
            else if (feature == 2)
            {
                if (top100OrRandom)
                    queryBuilder.Append(" ORDER BY T.TransactionDate DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
                else
                    queryBuilder.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");
            }

            using var connection = new SqlConnection(_connectionString);
            if (feature == 1 && pk_transactionIds != null && pk_transactionIds.Count > 0)
                return connection.Query<CenResTransactions>(queryBuilder.ToString(), new { Ids = pk_transactionIds });
            else
                return connection.Query<CenResTransactions>(queryBuilder.ToString());
        }

        public List<DbCountRow> GetCenResDbCountRows(string cendynPropertyId = null)
        {
            var sql = @"
        SELECT CendynPropertyId, 'Last 3 years' AS Range, 'Profiles' AS TableName, COUNT(1) AS Count
        FROM Profiles
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
            AND DatePMSProfileCreated >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CendynPropertyId
        UNION ALL
        SELECT CendynPropertyId, 'All time', 'Profiles', COUNT(1)
        FROM Profiles
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
        GROUP BY CendynPropertyId
        UNION ALL
        SELECT CendynPropertyId, 'Last 3 years', 'Reservations', COUNT(1)
        FROM Reservations
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
            AND DateResMade >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CendynPropertyId
        UNION ALL
        SELECT CendynPropertyId, 'All time', 'Reservations', COUNT(1)
        FROM Reservations
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
        GROUP BY CendynPropertyId
        UNION ALL
        SELECT R.CendynPropertyId, 'Last 3 years', 'StayDetail', COUNT(1)
        FROM StayDetail SD WITH (NOLOCK)
            INNER JOIN StayDetailHeader SH WITH(NOLOCK) ON SH.PK_StayDetailHeader = SD.FK_StayDetailHeader
            INNER JOIN Reservations R WITH(NOLOCK) ON R.PK_Reservations = SH.FK_Reservations
        WHERE (@CendynPropertyId IS NULL OR R.CendynPropertyId = @CendynPropertyId)
            AND SD.DateInserted >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY R.CendynPropertyId
        UNION ALL
        SELECT R.CendynPropertyId, 'All time', 'StayDetail', COUNT(1)
        FROM StayDetail SD WITH (NOLOCK)
            INNER JOIN StayDetailHeader SH WITH(NOLOCK) ON SH.PK_StayDetailHeader = SD.FK_StayDetailHeader
            INNER JOIN Reservations R WITH(NOLOCK) ON R.PK_Reservations = SH.FK_Reservations
        WHERE (@CendynPropertyId IS NULL OR R.CendynPropertyId = @CendynPropertyId)
        GROUP BY R.CendynPropertyId
        UNION ALL
        SELECT CendynPropertyId, 'Last 3 years', 'Transactions', COUNT(1)
        FROM Transactions WITH (NOLOCK)
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
            AND TransactionDate >= DATEADD(YEAR, -3, GETDATE())
        GROUP BY CendynPropertyId
        UNION ALL
        SELECT CendynPropertyId, 'All time', 'Transactions', COUNT(1)
        FROM Transactions WITH (NOLOCK)
        WHERE (@CendynPropertyId IS NULL OR CendynPropertyId = @CendynPropertyId)
        GROUP BY CendynPropertyId
    ";

            using var connection = new SqlConnection(_connectionString);
            return connection.Query<DbCountRow>(sql, new { CendynPropertyId = cendynPropertyId }).ToList();
        }

    }
}