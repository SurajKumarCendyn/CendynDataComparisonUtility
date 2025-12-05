using CendynDataComparisonUtility.Data;
using CendynDataComparisonUtility.Models;
using CendynDataComparisonUtility.Models.CenResDb;
using CendynDataComparisonUtility.Models.ClientDb;
using CendynDataComparisonUtility.Service;
using CendynDataComparisonUtility.Utility;
using ClosedXML.Excel;
using Dapper;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MongoDB.Driver;
using Newtonsoft.Json;
using System.Text;
using static CendynDataComparisonUtility.Data.MongoDbRepository;
using static CendynDataComparisonUtility.Service.CenResNormalizeDbRepository;

namespace CendynDataComparisonUtility.Controllers
{
    public class UtilityController : Controller
    {
        private readonly IConfiguration _config;
        public List<DatabaseInfo> Databases { get; set; } = new();
        readonly string[] dbsList = ["eInAppDb", "CenResDb", "MongoDb", "CenResNormalizeDb"];
        public UtilityController(IConfiguration config)
        {
            _config = config; 
        }
        public IActionResult Index(string searchString)
        {
            List<AvailableConnectionInformation> avlConnection = new();

            if (!string.IsNullOrEmpty(searchString))
            {
                // Search the company in all environments for both DBs
                foreach (var db in dbsList)
                {
                    var section = _config.GetSection(db);
                    foreach (var env in section.GetChildren())
                    {
                        //Select Data from mongodb 
                        if (db == "MongoDb")
                        {
                            var mongoDbconnStr = env.GetValue<string>("ConnectionString");
                            var mongoDbName = env.GetValue<string>("DatabaseName");

                            // Parse server name from connection string
                            var mongoUrl = new MongoUrl(mongoDbconnStr);
                            string mongoServerName = mongoUrl.Server.Host;

                            var client = new MongoClient(mongoDbconnStr);
                            var database = client.GetDatabase(mongoDbName);
                            var accountsColl = database.GetCollection<Models.MongoDb.Accounts>("accounts");
                            var filter = Builders<Models.MongoDb.Accounts>.Filter.Regex(a => a.CompanyName, new MongoDB.Bson.BsonRegularExpression(searchString, "i"));
                            var accounts = accountsColl.Find(filter).ToList();
                            foreach (var account in accounts)
                            {
                                string parentCompanyId = account.Id;
                                string parentCompanyName = account.CompanyName;

                                avlConnection.Add(new AvailableConnectionInformation()
                                {
                                    ParentCompanyId = parentCompanyId,
                                    CompanyName = parentCompanyName,
                                    ServerName = mongoServerName,
                                    DatabaseName = mongoDbName,
                                    DatabaseCType = "MongoDb",
                                    Environment = env.Key,
                                    ConnectionString = mongoDbconnStr
                                });
                            }
                            continue;
                        }

                        var connStr = env.GetValue<string>("ConnectionString");
                        var connstrBuilder = new SqlConnectionStringBuilder(connStr);
                        if (string.IsNullOrEmpty(connStr)) continue;
                        string query = db switch
                        {
                            "eInAppDb" => "SELECT Id AS ParentCompanyId, ParentCompany AS ParentCompanyName FROM [dbo].[CendynAdmin_ParentCompany] WITH(NOLOCK) WHERE ParentCompany LIKE @search",  //AppDb einsight
                            "CenResDb" => string.Empty,   //CenResDb Cendyn Manager
                            "CenResNormalizeDb" => "SELECT ParentCompanyId, ParentCompanyName FROM [CCRM].[ParentCompany] WITH(NOLOCK) WHERE ParentCompanyName LIKE @search", //CenResNormalizeDb Cendyn Normalize
                            _ => string.Empty
                        };

                        if (string.IsNullOrEmpty(query)) continue;

                        using var connection = new SqlConnection(connStr);
                        connection.Open();
                        var results = connection.Query<(string ParentCompanyId, string ParentCompanyName)>(
                            query,
                            new { search = $"%{searchString}%" }
                        );

                        foreach (var result in results)
                        {
                            string parentCompanyId = result.ParentCompanyId;
                            string parentCompanyName = result.ParentCompanyName;


                            if (db == "eInAppDb")
                            {
                                using var conn = new SqlConnection(connStr);
                                conn.Open();

                                // Use QueryMultiple to fetch both EINClientDb and CenResDb in one go
                                var sql = @"SELECT TOP 1 Id AS ParentCompanyId,ParentCompany as Companyname, CRM_SERVER as ServerName, CRM_Database as DatabaseName, CRM_User AS DatabaseUser, CRM_Password AS DatabasePassword, 'EINClientDb' AS DatabaseCType 
                                        FROM V_CRMCONNECTIONS WITH(NOLOCK) WHERE Id=@ParentCompanyId;
                                        SELECT TOP 1 Id AS ParentCompanyId, ParentCompany as Companyname, CenRes_Server as ServerName, CenRes_Database as DatabaseName, CenRes_User AS DatabaseUser, CenRes_Password AS DatabasePassword, 'CenResDb' AS DatabaseCType 
                                        FROM V_CRMCONNECTIONS WITH(NOLOCK) WHERE Id=@ParentCompanyId;";

                                using var multi = conn.QueryMultiple(sql, new { ParentCompanyId = parentCompanyId });

                                var eInsightClientDb = multi.Read<AvailableConnectionInformation>().FirstOrDefault();
                                if (eInsightClientDb != null)
                                {
                                    avlConnection.Add(eInsightClientDb);
                                }

                                var cenResDb = multi.Read<AvailableConnectionInformation>().FirstOrDefault();
                                if (cenResDb != null)
                                {
                                    avlConnection.Add(cenResDb);
                                }
                            }
                            if (db == "CenResNormalizeDb")
                            {
                                avlConnection.Add(new AvailableConnectionInformation()
                                {
                                    ParentCompanyId = parentCompanyId,
                                    CompanyName = parentCompanyName,
                                    ServerName = connstrBuilder.DataSource,
                                    DatabaseName = connstrBuilder.InitialCatalog,
                                    DatabaseCType = "CenResNormalizeDb",
                                    Environment = env.Key,
                                    ConnectionString = connStr
                                });
                            }
                        }
                    }
                }
            }
            GetConfiguredDatabases();
            TempData["AvlConnection"] = JsonConvert.SerializeObject(avlConnection);
            var viewModel = new UtilityViewModel()
            {
                SearchString = string.IsNullOrEmpty(searchString) ? string.Empty : searchString,
                DatabaseInfo = Databases,
                ConnectionInformation = avlConnection
            };
            return View(viewModel);
        }

        public void GetConfiguredDatabases()
        {
            foreach (var db in dbsList)
            {
                var section = _config.GetSection(db);
                foreach (var env in section.GetChildren())
                {
                    try
                    {
                        if (db == "MongoDb")
                        {
                            var mongoDbconnStr = env.GetValue<string>("ConnectionString");
                            if (!string.IsNullOrEmpty(mongoDbconnStr))
                            {
                                var mongoUrl = new MongoUrl(mongoDbconnStr);
                                Databases.Add(new DatabaseInfo
                                {
                                    DbType = db,
                                    Environment = env.Key,
                                    Name = mongoUrl.DatabaseName,
                                    ServerName = mongoUrl.Server.Host
                                });
                            }
                        }
                        else
                        {
                            var connStr = env.GetValue<string>("ConnectionString");
                            if (!string.IsNullOrEmpty(connStr))
                            {
                                var connstrBuilder = new SqlConnectionStringBuilder(connStr);
                                Databases.Add(new DatabaseInfo
                                {
                                    DbType = db,
                                    Environment = env.Key,
                                    Name = connstrBuilder.InitialCatalog,
                                    ServerName = connstrBuilder.DataSource
                                });
                            }
                        }
                    }
                    catch
                    {
                        // Optionally log the error or handle as needed
                        continue;
                    }
                }
            }
        }

        public IActionResult DataComparison(string searchString, int feature)
        {
            if (string.IsNullOrEmpty(searchString))
            {
                return RedirectToAction("Index");
            }
            if (feature < 0 || feature > 3)
            {
                return RedirectToAction("Index");
            }

            if (feature == 1)
            {
                var avlConnection = JsonConvert.DeserializeObject<List<AvailableConnectionInformation>>(TempData["AvlConnection"].ToString());
                var viewModel = new UtilityViewModel()
                {
                    SearchString = string.IsNullOrEmpty(searchString) ? string.Empty : searchString,
                    DatabaseInfo = Databases,
                    ConnectionInformation = avlConnection
                };
                return View(viewModel);
            }
            return RedirectToAction("Index");
        }
        [HttpPost]
        public FileContentResult VolumeBasedResult(RecordSelectionModel model)
        {
            //Get CenRes ReservationCount
            var avlConnection = JsonConvert.DeserializeObject<List<AvailableConnectionInformation>>(TempData["AvlConnection"].ToString());

            string cenResProfilesQuery = "SELECT COUNT(1) FROM V_PROFILES WITH(NOLOCK)"; //CenRes Profiles Counts
            string cenResReservationCountQuery = "SELECT COUNT(1) FROM V_RESERVATIONS WITH(NOLOCK)"; //CenRes Reservation Counts
            string cenResNightlyRatesQuery = "SELECT COUNT(1) FROM V_StayDetail WITH(NOLOCK)";  //CenRes Nightly Rates Counts
            string cenResTransactionsQuery = "SELECT COUNT(1) FROM V_StayDetail WITH(NOLOCK)"; //CenRes Transactions Counts
            if (model.TimeFrame == TimeFrame.Last3Years)
            {
                cenResProfilesQuery += " WHERE DateInserted >= DATEADD(YEAR, -3, GETDATE())";
                cenResReservationCountQuery += " WHERE DateInserted >= DATEADD(YEAR, -3, GETDATE())";
                cenResNightlyRatesQuery += " WHERE DateInserted >= DATEADD(YEAR, -3, GETDATE())";
                cenResTransactionsQuery += " WHERE DateInserted >= DATEADD(YEAR, -3, GETDATE())";
            }
            string cenresConnStr = FormatConnectionString(avlConnection.FirstOrDefault(c => c.DatabaseCType == "CenResDb"));
            using var cenresConnection = new SqlConnection(cenresConnStr);
            cenresConnection.Open();
            var profileCount = cenresConnection.ExecuteScalar<int>(cenResProfilesQuery);
            var reservationCount = cenresConnection.ExecuteScalar<int>(cenResReservationCountQuery);
            var nightlyRatesCount = cenresConnection.ExecuteScalar<int>(cenResNightlyRatesQuery);
            var transactionsCount = cenresConnection.ExecuteScalar<int>(cenResTransactionsQuery);


            ////Get Counts from MongoDb
            //var mongoConnInfo = avlConnection.FirstOrDefault(c => c.DatabaseCType == "MongoDb");
            //var mongoClient = new MongoClient(mongoConnInfo.ConnectionString);
            //string mongoDbName = mongoConnInfo.DatabaseName.Replace("metadata_", "");
            //var db = mongoClient.GetDatabase(mongoDbName);

            //var profilesCollection = db.GetCollection<Models.MongoDb.Contacts>("contacts");
            //var builder = Builders<Models.MongoDb.Contacts>.Filter;
            //var filter = builder.Eq(c => c.AccountId, mongoConnInfo.ParentCompanyId);

            //if (model.TimeFrame == TimeFrame.Last3Years)
            //{
            //    var dateFilter = builder.Gte("d", DateTime.UtcNow.AddYears(-3));
            //    filter = builder.And(filter, dateFilter);
            //}

            //var mongoProfileCount = profilesCollection.CountDocuments(filter);

            ////purchases count
            //var purchasesCollection = db.GetCollection<Models.MongoDb.Purchases>("purchases");
            //var purchaseFilterbuilder = Builders<Models.MongoDb.Purchases>.Filter;
            //var purchaseFilter = purchaseFilterbuilder.Eq(p => p.AccountId, mongoConnInfo.ParentCompanyId);
            //if (model.TimeFrame == TimeFrame.Last3Years)
            //{
            //    var dateFilter = purchaseFilterbuilder.Gte("d", DateTime.UtcNow.AddYears(-3));
            //    purchaseFilter = purchaseFilterbuilder.And(purchaseFilter, dateFilter);
            //}
            //var mongoPurchasesCount = purchasesCollection.CountDocuments(purchaseFilter);

            using var wb = new XLWorkbook();
            var ws = wb.Worksheets.Add("Volume Based Report");
            ws.Cell(1, 1).Value = "Data Type";
            ws.Cell(1, 2).Value = "CenResDb Counts";
            ws.Cell(1, 3).Value = "MongoDb Counts";
            ws.Cell(1, 4).Value = "NormalizeDb Counts";
            //Profile Data Counts
            ws.Cell(2, 1).Value = "Profiles";
            ws.Cell(2, 2).Value = profileCount;
            ws.Cell(2, 3).Value = "MongoProfileCount";
            ws.Cell(2, 4).Value = "NormalizeProfileCount";
            //Reservation Data Counts
            ws.Cell(3, 1).Value = "Reservations";
            ws.Cell(3, 2).Value = reservationCount;
            ws.Cell(3, 3).Value = "MongoReservationsCount";
            ws.Cell(3, 4).Value = "NormalizeReservationsCount";
            //Nightly Rates Data Counts
            ws.Cell(4, 1).Value = "Nightly Rates";
            ws.Cell(4, 2).Value = nightlyRatesCount;
            ws.Cell(4, 3).Value = "MongoNightlyRatesCount";
            ws.Cell(4, 4).Value = "NormalizeNightlyRatesCount";
            //Transactions Data Counts
            ws.Cell(5, 1).Value = "Transactions";
            ws.Cell(5, 2).Value = transactionsCount;
            ws.Cell(5, 3).Value = "MongoTransactionsCount";
            ws.Cell(5, 4).Value = "NormalizeTransactionsCount";

            ws.Columns().AdjustToContents();
            using var stream = new MemoryStream();
            wb.SaveAs(stream);
            stream.Position = 0;
            return File(stream.ToArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "VolumeBasedReport.xlsx");
        }

        [HttpGet]
        public IActionResult GetNewResult()
        {
            //SELECT TOP 100 Records from CenResDb for Tables Profiles, Reservations, StayDetails, Transactions
            var cenRes_ProfileQuery = new StringBuilder(QueryDefinitions.CenResDb.Profiles);
            cenRes_ProfileQuery.Append(" WHERE CendynPropertyId='1054'"); //Hardcode for testing
            cenRes_ProfileQuery.Append(@" GROUP BY
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
            cenRes_ProfileQuery.Append(" ORDER BY P.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            var cenRes_ProfileRandom100Query = new StringBuilder(QueryDefinitions.CenResDb.Profiles);
            cenRes_ProfileRandom100Query.Append(" WHERE CendynPropertyId='1054'");
            cenRes_ProfileRandom100Query.Append(@" GROUP BY
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
            cenRes_ProfileRandom100Query.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            var cenRes_ReservationQuery = new StringBuilder(QueryDefinitions.CenResDb.Reservations);
            cenRes_ReservationQuery.Append(" WHERE CendynPropertyId='1054'"); 
            cenRes_ReservationQuery.Append(" ORDER BY R.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            var cenRes_ReservationRandom100Query = new StringBuilder(QueryDefinitions.CenResDb.Reservations);
            cenRes_ReservationRandom100Query.Append(" WHERE CendynPropertyId='1054'");
            cenRes_ReservationRandom100Query.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

           var cenRes_StayDetailQuery = new StringBuilder(QueryDefinitions.CenResDb.StayDetail);
           cenRes_StayDetailQuery.Append(" WHERE R.CendynPropertyId='1054'");
            cenRes_StayDetailQuery.Append(" ORDER BY SD.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            var cenRes_StayDetailRandom100Query = new StringBuilder(QueryDefinitions.CenResDb.StayDetail);
            cenRes_StayDetailRandom100Query.Append(" WHERE R.CendynPropertyId='1054'");
            cenRes_StayDetailRandom100Query.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");


            var cenRes_TransactionsQuery = new StringBuilder(QueryDefinitions.CenResDb.Transactions);
            cenRes_TransactionsQuery.Append(" WHERE CendynPropertyId='1054'");
            cenRes_TransactionsQuery.Append(" ORDER BY T.DateInserted DESC OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            var cenRes_TransactionsRandom100Query = new StringBuilder(QueryDefinitions.CenResDb.Transactions);
            cenRes_TransactionsRandom100Query.Append(" WHERE CendynPropertyId='1054'");
            cenRes_TransactionsRandom100Query.Append(" ORDER BY NEWID() OFFSET 0 ROWS FETCH NEXT 100 ROWS ONLY");

            //select multi data using dapper from cenres db
            string CenRes_connectionString = "Server=QDB-D1007.CENTRALSERVICES.LOCAL;Database=CenRes_QA_Test;Integrated Security=True;TrustServerCertificate=True;";
            using var connection = new SqlConnection(CenRes_connectionString);
            connection.Open();
            using var multi = connection.QueryMultiple(
                cenRes_ProfileQuery.ToString() + ";" +
                cenRes_ProfileRandom100Query.ToString() + ";" +
                cenRes_ReservationQuery.ToString() + ";" +
                cenRes_ReservationRandom100Query.ToString() + ";" +
                cenRes_StayDetailQuery.ToString() + ";" +
                cenRes_StayDetailRandom100Query.ToString() + ";" +
                cenRes_TransactionsQuery.ToString() + ";" +
                cenRes_TransactionsRandom100Query.ToString());

            var cenRes_Profiles = multi.Read<CenResProfiles>().ToList();
            var cenRes_ProfilesRandom100 = multi.Read<CenResProfiles>().ToList();
            var cenRes_Reservations = multi.Read<CenResReservations>().ToList();
            var cenRes_ReservationsRandom100 = multi.Read<CenResReservations>().ToList();
            var cenRes_StayDetails = multi.Read<CenResStayDetail>().ToList();
            var cenRes_StayDetailsRandom100 = multi.Read<CenResStayDetail>().ToList();
            var cenRes_Transactions = multi.Read<CenResTransactions>().ToList();
            var cenRes_TransactionsRandom100 = multi.Read<CenResTransactions>().ToList();





            return View();
        }

        /// <summary>
        /// Compare Data from EInDb >> CenResDb >> MongoDb >> NormalizeDb
        /// </summary>
        /// <returns></returns> 
        [HttpGet("Utility/CompareData")]
        public FileContentResult CompareData([FromQuery] int featureSet = 1, [FromQuery] string searchString = null)
        { 
            using var wb = new XLWorkbook();
            CreateProfilesSheet(wb, featureSet);
            CreateReservationsSheet(wb, featureSet);
            CreateStayDetailsSheet(wb, featureSet);
            CreateTransactionsSheet(wb, featureSet);
            using var stream = new MemoryStream();
            wb.SaveAs(stream);
            stream.Position = 0;
            return File(stream.ToArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "DataComparison.xlsx");
        }
         
        private static void CreateProfilesSheet(XLWorkbook wb, int featureSet)
        {
            string parentCompanyId = "67371b9bd167a7000161f496";
            var (eInRepo, cenResRepo, mongoRepo, cenResNRepo) = CreateAllRepositories();

            #region Profiles Comparison
            IEnumerable<Customer> eIn_Customer = null;
            IEnumerable<CenResProfiles> cenRes_Profiles = null;

            switch (featureSet)
            {
                case 1:
                    // Start from eInsight
                    eIn_Customer = eInRepo.GetCustomers();
                    var profileIds = eIn_Customer.Select(c => c.PK_Profiles).ToList();
                    cenRes_Profiles = cenResRepo.GetProfiles(profileIds, featureSet);
                    break;
                case 2:
                    // Start from CenRes
                    cenRes_Profiles = cenResRepo.GetProfiles(null, 2);
                    break;
                default:
                    // No valid featureSet, leave both as null
                    break;
            }
            List<MongoCenResMap> userIds = [.. cenRes_Profiles.Select(p => new MongoCenResMap { UserIds = BuildMongoUserId(p), Pk_Profiles = p.PK_Profiles })];
            var mongoProfiles = mongoRepo.GetContacts(userIds, parentCompanyId);

            var mongoCustomerIds = mongoProfiles.Select(m => m.Id).ToList();
            var cenResNCustomers = cenResNRepo.GetCustomers(parentCompanyId, mongoCustomerIds);
            //Add Static Pk_profiles for reference to cenResNCustomers
            foreach (var normCust in cenResNCustomers)
            {
                var mongoProfile = mongoProfiles.FirstOrDefault(m => m.Id == normCust.CustomerId);
                normCust.Pk_Profiles = mongoProfile != null ? mongoProfile.Pk_Profiles : Guid.Empty;
            }
            #endregion

            // Collect keys from each source
            var eInKeys = eIn_Customer?.Select(c => c.PK_Profiles.ToString());
            var cenKeys = cenRes_Profiles?.Select(p => p.PK_Profiles.ToString());
            var mongoKeys = mongoProfiles?.Select(m => m.Pk_Profiles.ToString()); // Adjust property if needed
            var normKeys = cenResNCustomers?.Select(n => n.Pk_Profiles.ToString());

            // Union all keys and get distinct values
            var allKeys = (eInKeys ?? [])
                        .Concat(cenKeys ?? [])
                        .Concat(mongoKeys ?? [])
                        .Concat(normKeys ?? [])
                        .Where(k => !string.IsNullOrEmpty(k))
                        .Distinct()
                        .ToList();

            var comparisonRows = new List<FieldComparisonRow>();
            foreach (var key in allKeys)
            {
                var ein = eIn_Customer?.FirstOrDefault(x => x.PK_Profiles == new Guid(key));
                var cen = cenRes_Profiles?.FirstOrDefault(x => x.PK_Profiles == new Guid(key));
                var mongo = mongoProfiles?.FirstOrDefault(x => x.Pk_Profiles == new Guid(key));
                var norm = cenResNCustomers?.FirstOrDefault(x => x.Pk_Profiles == new Guid(key));

                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "First Name",
                    EInDbValue = ein?.FirstName,
                    CenResDbValue = cen?.FirstName,
                    MongoDbValue = mongo?.FirstName,
                    CenResNormalizeDbValue = norm?.FirstName
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Last Name",
                    EInDbValue = ein?.LastName,
                    CenResDbValue = cen?.LastName,
                    MongoDbValue = mongo?.LastName,
                    CenResNormalizeDbValue = norm?.LastName
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Salutation",
                    EInDbValue = ein?.Salutation,
                    CenResDbValue = cen?.Salutation,
                    MongoDbValue = mongo?.Salutation,
                    CenResNormalizeDbValue = norm?.Salutation
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Address1",
                    EInDbValue = ein?.Address1,
                    CenResDbValue = cen?.Address1,
                    MongoDbValue = mongo?.Address1,
                    CenResNormalizeDbValue = norm?.Address1
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "City",
                    EInDbValue = ein?.City,
                    CenResDbValue = cen?.City,
                    MongoDbValue = mongo?.City,
                    CenResNormalizeDbValue = norm?.City
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "StateProvinceCode",
                    EInDbValue = ein?.StateProvinceCode,
                    CenResDbValue = cen?.StateProvince,
                    MongoDbValue = "No value",
                    CenResNormalizeDbValue = norm?.StateProvince
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "ZipCode",
                    EInDbValue = ein?.ZipCode,
                    CenResDbValue = cen?.PostalCode,
                    MongoDbValue = mongo?.PostalCode,
                    CenResNormalizeDbValue = norm?.ZipCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "CountryCode",
                    EInDbValue = ein?.CountryCode,
                    CenResDbValue = cen?.CountryCode,
                    MongoDbValue = mongo?.CountryCode,
                    CenResNormalizeDbValue = norm?.CountryCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "PhoneNumber",
                    EInDbValue = ein?.PhoneNumber,
                    CenResDbValue = cen?.PhoneNumber,
                    MongoDbValue = mongo?.PhoneNumber,
                    CenResNormalizeDbValue = norm?.PhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "HomePhoneNumber",
                    EInDbValue = ein?.HomePhoneNumber,
                    CenResDbValue = cen?.HomePhone,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.HomePhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "FaxNumber",
                    EInDbValue = ein?.FaxNumber,
                    CenResDbValue = cen?.FaxNumber,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.FaxNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Email",
                    EInDbValue = ein?.Email,
                    CenResDbValue = cen?.Email,
                    MongoDbValue = mongo?.Email,
                    CenResNormalizeDbValue = norm?.Email
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Languages",
                    EInDbValue = ein?.Languages,
                    CenResDbValue = cen?.Language,
                    MongoDbValue = mongo?.Language,
                    CenResNormalizeDbValue = norm?.Languages
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "Nationality",
                    EInDbValue = ein?.Nationality,
                    CenResDbValue = cen?.Nationality,
                    MongoDbValue = mongo?.Nationality,
                    CenResNormalizeDbValue = norm?.Nationality
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "CellPhoneNumber",
                    EInDbValue = ein?.CellPhoneNumber,
                    CenResDbValue = "No Value",
                    MongoDbValue = "No value",
                    CenResNormalizeDbValue = norm?.CellPhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "BusinessPhoneNumber",
                    EInDbValue = ein?.BusinessPhoneNumber,
                    CenResDbValue = cen?.WorkPhone,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.BusinessPhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "CompanyTitle",
                    EInDbValue = ein?.CompanyTitle,
                    CenResDbValue = cen?.CompanyName,
                    MongoDbValue = mongo?.CompanyName,
                    CenResNormalizeDbValue = norm?.CompanyTitle
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "JobTitle",
                    EInDbValue = ein?.JobTitle,
                    CenResDbValue = cen?.JobTitle,
                    MongoDbValue = mongo?.JobTitle,
                    CenResNormalizeDbValue = norm?.JobTitle
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "AllowEMail",
                    EInDbValue = ein?.AllowEMail,
                    CenResDbValue = cen?.AllowEmail,
                    MongoDbValue = mongo?.AllowEmail,
                    CenResNormalizeDbValue = norm?.AllowEMail
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ProfileId = cen.ExternalProfileID,
                    FieldName = "AllowMail",
                    EInDbValue = ein?.AllowMail,
                    CenResDbValue = cen?.AllowMail,
                    MongoDbValue = mongo?.AllowMail,
                    CenResNormalizeDbValue = norm?.AllowMail
                });
            }
            var ws = wb.Worksheets.Add("Profiles");
            ws.Cell(1, 1).Value = "Key";
            ws.Cell(1, 2).Value = "PropertyId";
            ws.Cell(1, 3).Value = "ProfileId";
            ws.Cell(1, 4).Value = "Field";
            ws.Cell(1, 5).Value = "eInDb";
            ws.Cell(1, 6).Value = "CenResDb";
            ws.Cell(1, 7).Value = "MongoDb";
            ws.Cell(1, 8).Value = "CenResNormalizeDb";

            for (int i = 0; i < comparisonRows.Count; i++)
            {
                var row = comparisonRows[i];
                ws.Cell(i + 2, 1).Value = row.Key;
                ws.Cell(i + 2, 2).Value = row.PropertyId;
                ws.Cell(i + 2, 3).Value = row.ProfileId;
                ws.Cell(i + 2, 4).Value = row.FieldName;
                ws.Cell(i + 2, 5).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(i + 2, 6).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(i + 2, 7).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(i + 2, 8).Value = row?.CenResNormalizeDbValue ?? string.Empty;
            }
            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);


            // Highlight mismatches
            for (int i = 2; i <= comparisonRows.Count + 1; i++)
            {
                var ein = ws.Cell(i, 5).GetString();
                var cen = ws.Cell(i, 6).GetString();
                var mongo = ws.Cell(i, 7).GetString();
                var normalized = ws.Cell(i, 8).GetString();

                // eInDb
                if (!string.Equals(ein, cen, StringComparison.OrdinalIgnoreCase))
                    ws.Cell(i, 5).Style.Fill.BackgroundColor = XLColor.LightSalmon;
                else
                    ws.Cell(i, 5).Style.Fill.BackgroundColor = XLColor.LightGreen;

                //CenRes
                if (!string.Equals(cen, mongo, StringComparison.OrdinalIgnoreCase))
                    ws.Cell(i, 6).Style.Fill.BackgroundColor = XLColor.LightYellow;
                else
                    ws.Cell(i, 6).Style.Fill.BackgroundColor = XLColor.LightGreen;


                // MongoDb
                if (!string.Equals(mongo, normalized, StringComparison.OrdinalIgnoreCase))
                    ws.Cell(i, 7).Style.Fill.BackgroundColor = XLColor.LightYellow;
                else
                    ws.Cell(i, 7).Style.Fill.BackgroundColor = XLColor.LightGreen;

                // CenResNormalizeDb
                if (!string.Equals(mongo, normalized, StringComparison.OrdinalIgnoreCase))
                    ws.Cell(i, 8).Style.Fill.BackgroundColor = XLColor.LightYellow;
                else
                    ws.Cell(i, 8).Style.Fill.BackgroundColor = XLColor.LightGreen;
            }
        }
        private static void CreateReservationsSheet(XLWorkbook wb, int featureSet)
        {
            string parentCompanyId = "67371b9bd167a7000161f496";
            var (eInRepo, cenResRepo, mongoRepo, cenResNRepo) = CreateAllRepositories();

            #region Reservations Comparison
            IEnumerable<CustomerStay> eIn_Reservations = null;
            IEnumerable<CenResReservations> cenRes_Reservations = null;
            switch (featureSet)
            {
                case 1:
                    // Start from eInsight
                    eIn_Reservations = eInRepo.GetReservations();
                    var reservationIds = eIn_Reservations.Select(r => r.Pk_Reservations).ToList();
                    cenRes_Reservations = cenResRepo.GetReservations(reservationIds, 1);
                    break;
                case 2:
                    // Start from CenRes
                    cenRes_Reservations = cenResRepo.GetReservations(null, 2);
                    break;
                default:
                    // No valid featureSet, leave both as null
                    break;
            }
            var cenResIds = cenRes_Reservations.Select(r => new MongoResMap { ReservationNo = r.ReservationNumber, CendynPropertyId = r.CendynPropertyID }).ToList();
            var mongo_Reservations = mongoRepo.Purchases(cenResIds, parentCompanyId);

            var puchaseIds = mongo_Reservations.Select(r => r.Id).ToList();
            var cenResN_Reservations = cenResNRepo.GetReservation(parentCompanyId, puchaseIds);

            #endregion

            var ws = wb.Worksheets.Add("Reservations");
            ws.Cell(1, 1).Value = "Key";
            ws.Cell(1, 2).Value = "PropertyId";
            ws.Cell(1, 3).Value = "ReservationID";
            ws.Cell(1, 4).Value = "Field";
            ws.Cell(1, 5).Value = "eInDb";
            ws.Cell(1, 6).Value = "CenResDb";
            ws.Cell(1, 7).Value = "MongoDb";
            ws.Cell(1, 8).Value = "CenResNormalizeDb";

            // Collect keys from each source   // Resevation id is common in ein()
            var eInKeys = eIn_Reservations?.Select(c => c.ReservationNumber);
            var cenKeys = cenRes_Reservations?.Select(p => p.ReservationNumber);
            var mongoKeys = mongo_Reservations?.Select(m => m.UniqId_ExternalResID1);
            var normKeys = cenResN_Reservations?.Select(n => n.ReservationNumber);

            // Union all keys and get distinct values
            var allKeys = (eInKeys ?? [])
              .Concat(cenKeys ?? [])
              .Concat(mongoKeys ?? [])
              .Concat(normKeys ?? [])
              .Where(k => !string.IsNullOrEmpty(k))
              .Distinct()
              .ToList();

            var comparisonRows = new List<FieldComparisonRowReservations>();
            foreach (var key in allKeys) // union of all PKs from all sources
            {
                var ein = eIn_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);
                var cen = cenRes_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);
                var mongo = mongo_Reservations?.FirstOrDefault(x => x.UniqId_ExternalResID1 == key);
                var norm = cenResN_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);

                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "Sub Reservation Number",
                    EInDbValue = ein?.SubReservationNumber,
                    CenResDbValue = cen?.SubReservationNumber,
                    MongoDbValue = mongo?.ConfirmationNumber,
                    CenResNormalizeDbValue = norm?.SubReservationNumber
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "CentralReservation",
                    EInDbValue = ein?.CentralReservation,
                    CenResDbValue = cen?.CentralReservation,
                    MongoDbValue = mongo?.CentralResNum,
                    CenResNormalizeDbValue = norm?.CentralReservation
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "BookingEngConfNum",
                    EInDbValue = ein?.BookingEngConfNum,
                    CenResDbValue = cen?.BookingEngConfNum,
                    MongoDbValue = mongo?.BookingSourceName,
                    CenResNormalizeDbValue = norm?.SourceOfBusiness
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "StayStatus",
                    EInDbValue = ein?.StayStatus,
                    CenResDbValue = cen?.StayStatus,
                    MongoDbValue = mongo?.ResStatusCode,
                    CenResNormalizeDbValue = norm?.StayStatus
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "ArrivalDate",
                    EInDbValue = ein?.ArrivalDate.ToString(),
                    CenResDbValue = cen?.ArrivalDate.ToString(),
                    MongoDbValue = mongo?.ResArriveDate.ToString(),
                    CenResNormalizeDbValue = norm?.ArrivalDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "DepartureDate",
                    EInDbValue = ein?.DepartureDate.ToString(),
                    CenResDbValue = cen?.DepartureDate.ToString(),
                    MongoDbValue = mongo?.ResDepartDate.ToString(),
                    CenResNormalizeDbValue = norm?.DepartureDate.ToString()
                });

                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "BookingDate",
                    EInDbValue = ein?.BookingDate.ToString(),
                    CenResDbValue = cen?.BookingDate?.ToString(),
                    MongoDbValue = mongo?.BookingDate?.ToString(),
                    CenResNormalizeDbValue = norm?.BookingDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "CancelDate",
                    EInDbValue = ein?.CancelDate.ToString(),
                    CenResDbValue = cen?.CancelDate?.ToString(),
                    MongoDbValue = mongo?.CancelDate?.ToString(),
                    CenResNormalizeDbValue = norm?.CancelDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen?.ReservationNumber, // Use null-conditional operator
                    FieldName = "GroupReservation",
                    EInDbValue = ein?.GroupReservation?.ToString(), // Use null-conditional operator
                    CenResDbValue = cen?.GroupReservation?.ToString(), // Use null-conditional operator
                    MongoDbValue = mongo?.GroupName?.ToString(), // Use null-conditional operator
                    CenResNormalizeDbValue = norm?.GroupReservation?.ToString() // Use null-conditional operator
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "Channel",
                    EInDbValue = ein?.Channel?.ToString(),
                    CenResDbValue = cen?.Channel?.ToString(),
                    MongoDbValue = mongo?.StayChannelCode?.ToString(),
                    CenResNormalizeDbValue = norm?.Channel?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "SourceOfBusiness",
                    EInDbValue = ein?.SourceOfBusiness?.ToString(),
                    CenResDbValue = cen?.SourceOfBusiness?.ToString(),
                    MongoDbValue = mongo?.BookingSourceName,
                    CenResNormalizeDbValue = norm?.SourceOfBusiness?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "MarketSeg",
                    EInDbValue = ein?.MarketSeg?.ToString(),
                    CenResDbValue = cen?.MarketSeg?.ToString(),
                    MongoDbValue = mongo?.MarketSegmentCode,
                    CenResNormalizeDbValue = norm?.MarketSeg?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "MarketSubSeg",
                    EInDbValue = ein?.MarketSubSeg?.ToString(),
                    CenResDbValue = cen?.MarketSubSeg?.ToString(),
                    MongoDbValue = "",// mongo field not available
                    CenResNormalizeDbValue = norm?.MarketSubSeg?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "RoomNights",
                    EInDbValue = ein?.RoomNights.ToString(),
                    CenResDbValue = cen?.RoomNights.ToString(),
                    MongoDbValue = mongo?.NumOfNights.ToString(),
                    CenResNormalizeDbValue = norm?.RoomNights.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "NumAdults",
                    EInDbValue = ein?.NumAdults.ToString(),
                    CenResDbValue = cen?.NumAdults.ToString(),
                    MongoDbValue = mongo?.NumOfAdults.ToString(),
                    CenResNormalizeDbValue = norm?.NumAdults.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "NumChildren",
                    EInDbValue = ein?.NumChildren.ToString(),
                    CenResDbValue = cen?.NumChildren.ToString(),
                    MongoDbValue = mongo?.NumOfChildren.ToString(),
                    CenResNormalizeDbValue = norm?.NumChildren.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "TotalPersons",
                    EInDbValue = ein?.TotalPersons.ToString(),
                    CenResDbValue = cen?.TotalPersons.ToString(),
                    MongoDbValue = mongo?.TotalPersons.ToString(),
                    CenResNormalizeDbValue = norm?.TotalPersons.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "RateType",
                    EInDbValue = ein?.RateType?.ToString(),
                    CenResDbValue = cen?.RateType?.ToString(),
                    MongoDbValue = mongo?.RateType?.ToString(),
                    CenResNormalizeDbValue = norm?.RateType?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "RoomTypeCode",
                    EInDbValue = ein?.RoomTypeCode?.ToString(),
                    CenResDbValue = cen?.RoomTypeCode?.ToString(),
                    MongoDbValue = mongo?.RoomTypeCode?.ToString(),
                    CenResNormalizeDbValue = norm?.RoomTypeCode?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "RoomCode",
                    EInDbValue = ein?.RoomCode?.ToString(),
                    CenResDbValue = cen?.RoomCode?.ToString(),
                    MongoDbValue = mongo?.RoomCode?.ToString(),
                    CenResNormalizeDbValue = norm?.RoomCode?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "IATA",
                    EInDbValue = ein?.IATA?.ToString(),
                    CenResDbValue = cen?.IATA?.ToString(),
                    MongoDbValue = mongo?.TravelAgentIata?.ToString(),
                    CenResNormalizeDbValue = norm?.IATA?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "NumRooms",
                    EInDbValue = ein?.NumRooms.ToString(),
                    CenResDbValue = cen?.NumRooms.ToString(),
                    MongoDbValue = mongo?.NumberRooms.ToString(),
                    CenResNormalizeDbValue = norm?.NumRooms.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "RoomRevenue",
                    EInDbValue = ein?.RoomRevenue.ToString(),
                    CenResDbValue = cen?.RoomRevenue.ToString(),
                    MongoDbValue = mongo?.RoomRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.RoomRevenue.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "Tax",
                    EInDbValue = ein?.Tax.ToString(),
                    CenResDbValue = cen?.Tax.ToString(),
                    MongoDbValue = mongo?.TotalTax.ToString(),
                    CenResNormalizeDbValue = norm?.Tax.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "OtherRevenues",
                    EInDbValue = ein?.OtherRevenue.ToString(),
                    CenResDbValue = cen?.OtherRevenue.ToString(),
                    MongoDbValue = mongo?.TotalOtherRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.OtherRevenue.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowReservations
                {
                    Key = key?.ToString(),
                    PropertyId = cen?.CendynPropertyID,
                    ReservationId = cen.ReservationNumber,
                    FieldName = "TotalRevenue",
                    EInDbValue = ein?.TotalRevenue.ToString(),
                    CenResDbValue = cen?.TotalRevenue.ToString(),
                    MongoDbValue = mongo?.TotalRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.TotalRevenue.ToString()
                });
            }

            for (int i = 0; i < comparisonRows.Count; i++)
            {
                var row = comparisonRows[i];
                ws.Cell(i + 2, 1).Value = row.Key;
                ws.Cell(i + 2, 2).Value = row.PropertyId;
                ws.Cell(i + 2, 3).Value = row.ReservationId;
                ws.Cell(i + 2, 4).Value = row.FieldName;
                ws.Cell(i + 2, 5).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(i + 2, 6).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(i + 2, 7).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(i + 2, 8).Value = row?.CenResNormalizeDbValue ?? string.Empty;
            }
            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);
        }
        private static void CreateStayDetailsSheet(XLWorkbook wb, int featureSet)
        {
            string parentCompanyId = "67371b9bd167a7000161f496";
            var (eInRepo, cenResRepo, mongoRepo, cenResNRepo) = CreateAllRepositories();

            #region StayDetail Comparison 
            IEnumerable<CustomerStayDetail> eIn_StayDetails = null;
            IEnumerable<CenResStayDetail> cenRes_StayDetails = null;
            switch (featureSet)
            {
                case 1:
                    // Start from eInsight
                    eIn_StayDetails = eInRepo.GetStayDetails();
                    var stayIds = eIn_StayDetails.Select(s => s.Pk_StayDetail).ToList();
                    cenRes_StayDetails = cenResRepo.GetStayDetails(stayIds);
                    break;
                case 2:
                    // Start from CenRes
                    cenRes_StayDetails = cenResRepo.GetStayDetails(null, 2);
                    break;
                default:
                    // No valid featureSet, leave both as null
                    break;
            }
            var cenResIds = cenRes_StayDetails.Select(r => new MongoResMap { ReservationNo = r.ReservationNumber, CendynPropertyId = r.CendynPropertyId }).ToList();
            var mongo_StayDetails = mongoRepo.StayDetails(cenResIds, parentCompanyId);

            var mongo_StayIds = mongo_StayDetails.Select(s => s.Id).ToList();
            var cenResN_StayDetails = cenResNRepo.GetStayDetails(parentCompanyId, mongo_StayIds);
            var mongoRes_PropertyMap = mongo_StayDetails.Select(m => new MongoResMap { ReservationNo = m.UniqId_ExternalResID1, CendynPropertyId = m.Uuid_CendynPropertyID }).ToList();

            foreach (var cenResNStayDetail in cenResN_StayDetails)
            {
                // Use FirstOrDefault with a predicate for direct lookup
                cenResNStayDetail.CendynPropertyId = mongoRes_PropertyMap
                    .FirstOrDefault(y => y.ReservationNo == cenResNStayDetail.ReservationNumber)?.CendynPropertyId;
            }

            #endregion

            var ws = wb.Worksheets.Add("StayDetails");
            ws.Cell(1, 1).Value = "Key";
            ws.Cell(1, 2).Value = "PropertyId";
            ws.Cell(1, 3).Value = "ReservationId";
            ws.Cell(1, 4).Value = "Stay Date";
            ws.Cell(1, 5).Value = "Field";
            ws.Cell(1, 6).Value = "eInDb";
            ws.Cell(1, 7).Value = "CenResDb";
            ws.Cell(1, 8).Value = "MongoDb";
            ws.Cell(1, 9).Value = "CenResNormalizeDb";

            // Collect keys from each source   // Resevation id is common in ein()
            var eInKeys = eIn_StayDetails?.Select(c => c.ReservationNumber);
            var cenKeys = cenRes_StayDetails?.Select(p => p.ReservationNumber);
            var mongoKeys = mongo_StayDetails?.Select(m => m.UniqId_ExternalResID1);
            var normKeys = cenResN_StayDetails?.Select(n => n.ReservationNumber);

            // Union all keys and get distinct values
            var allKeys = (eInKeys ?? [])
              .Concat(cenKeys ?? [])
              .Concat(mongoKeys ?? [])
              .Concat(normKeys ?? [])
              .Where(k => !string.IsNullOrEmpty(k))
              .Distinct()
              .ToList();

            var comparisonRows = new List<FieldComparisonRowStayDetail>();
            foreach (var key in allKeys)
            {
                var ein = eIn_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);
                var cen = cenRes_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);
                var mongo = mongo_StayDetails?.FirstOrDefault(x => x.UniqId_ExternalResID1 == key);
                var norm = cenResN_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);

                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "StayDate",
                    EInDbValue = ein?.StayDate.ToString(),
                    CenResDbValue = cen?.StayDate.ToString(),
                    MongoDbValue = mongo?.StayDate.ToString(),
                    CenResNormalizeDbValue = norm?.StayDate.ToString()
                });

                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "StayRateType",
                    EInDbValue = ein?.StayRateType,
                    CenResDbValue = cen?.StayRateType,
                    MongoDbValue = mongo?.RateType,
                    CenResNormalizeDbValue = norm?.StayRateType
                });

                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "StayRoomType",
                    EInDbValue = ein?.StayRoomType,
                    CenResDbValue = cen?.StayRoomType,
                    MongoDbValue = mongo?.RoomType,
                    CenResNormalizeDbValue = norm?.StayRoomType
                });
                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "StayRateAmount",
                    EInDbValue = ein?.StayRateAmount.ToString(),
                    CenResDbValue = cen?.StayRateAmount.ToString(),
                    MongoDbValue = mongo?.StayRateAmount.ToString(),
                    CenResNormalizeDbValue = norm?.StayRateAmount.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "StayNumRooms",
                    EInDbValue = ein?.StayNumRooms.ToString(),
                    CenResDbValue = cen?.NumberOfRooms.ToString(),
                    MongoDbValue = mongo?.NumberOfRooms.ToString(),
                    CenResNormalizeDbValue = norm?.StayNumRooms.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowStayDetail
                {
                    Key = key.ToString(),
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen.ReservationNumber,
                    StayDate = ein?.StayDate.ToString(),
                    FieldName = "CurrencyCode",
                    EInDbValue = ein?.CurrencyCode.ToString(),
                    CenResDbValue = cen?.CurrencyCode.ToString(),
                    MongoDbValue = mongo?.CurrencyCode.ToString(),
                    CenResNormalizeDbValue = norm?.CurrencyCode.ToString()
                });
            }

            for (int i = 0; i < comparisonRows.Count; i++)
            {
                var row = comparisonRows[i];
                ws.Cell(i + 2, 1).Value = row.Key;
                ws.Cell(i + 2, 2).Value = row.PropertyId;
                ws.Cell(i + 2, 3).Value = row.ReservationId;
                ws.Cell(i + 2, 4).Value = row.StayDate;
                ws.Cell(i + 2, 5).Value = row.FieldName;
                ws.Cell(i + 2, 6).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(i + 2, 7).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(i + 2, 8).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(i + 2, 9).Value = row?.CenResNormalizeDbValue ?? string.Empty;
            }
            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);
        }
        private static void CreateTransactionsSheet(XLWorkbook wb, int featureSet)
        {
            string parentCompanyId = "67371b9bd167a7000161f496";
            var (eInRepo, cenResRepo, mongoRepo, cenResNRepo) = CreateAllRepositories();
            #region Transactions Comparison 
            IEnumerable<CustomerTransactions> eIn_Transaction = null;
            IEnumerable<CenResTransactions> cenRes_Transactions = null;

            switch (featureSet)
            {
                case 1:
                    // Start from eInsight
                    eIn_Transaction = eInRepo.GetTransactions();
                    var transactionIds = eIn_Transaction.Select(t => t.PK_Transactions).ToList();
                    cenRes_Transactions = cenResRepo.GetTransactions(transactionIds);
                    break;
                case 2:
                    // Start from CenRes
                    cenRes_Transactions = cenResRepo.GetTransactions(null, 2);
                    break;
                default:
                    // No valid featureSet, leave both as null
                    break;
            }

            var mongoTransMap = cenRes_Transactions.Select(r =>
            new MongoTransactionMap
            {
                Pk_Transactions = r.PK_Transactions,
                ExternalResId1 = r.ExternalResID1,
                TransactionId = r.TransactionId
            }).ToList();

            var mongo_Transactions = mongoRepo.Transactions(mongoTransMap, parentCompanyId);
            foreach (var mongoTransaction in mongo_Transactions)
            {
                mongoTransaction.Pk_Transactions = mongoTransMap
                    .FirstOrDefault(y => y.TransactionId == mongoTransaction.TransactionId && y.ExternalResId1 == mongoTransaction.ExternalResId1).Pk_Transactions;
            }

            var mongo_TransactionIds = mongo_Transactions.Select(t => new MongoTransactionMapForCenResNDb { Mongo_TransactionId = t.Id, Pk_transactions = t.Pk_Transactions }).ToList();
            var cenResN_Transactions = cenResNRepo.GetTransactions(parentCompanyId, mongo_TransactionIds);
            #endregion

            var ws = wb.Worksheets.Add("Transactions");
            ws.Cell(1, 1).Value = "PropertyId";
            ws.Cell(1, 2).Value = "ReservationId";
            ws.Cell(1, 3).Value = "TransactionId";
            ws.Cell(1, 4).Value = "TransactionDate";
            ws.Cell(1, 5).Value = "Field";
            ws.Cell(1, 6).Value = "eInDb";
            ws.Cell(1, 7).Value = "CenResDb";
            ws.Cell(1, 8).Value = "MongoDb";
            ws.Cell(1, 9).Value = "CenResNormalizeDb";

            // Collect keys from each source
            var eInKeys = eIn_Transaction?.Select(p => p.PK_Transactions.ToString());
            var cenKeys = cenRes_Transactions?.Select(p => p.PK_Transactions.ToString());
            var mongoKeys = mongo_TransactionIds?.Select(m => m.Pk_transactions.ToString());
            var normKeys = cenResN_Transactions?.Select(n => n.Pk_Transactions.ToString());

            // Safely handle possible null for eInKeys
            var allKeys = (eInKeys ?? [])
                .Concat(cenKeys ?? [])
                .Concat(mongoKeys ?? [])
                .Concat(normKeys ?? [])
                .Where(k => !string.IsNullOrEmpty(k))
                .Distinct()
                .ToList();

            var comparisonRows = new List<FieldComparisonRowTransaction>();
            foreach (var key in allKeys) // union of all PKs from all sources
            {
                var ein = eIn_Transaction?.FirstOrDefault(x => x.PK_Transactions == new Guid(key));
                var cen = cenRes_Transactions?.FirstOrDefault(x => x.PK_Transactions == new Guid(key));
                var mongo = mongo_Transactions?.FirstOrDefault(x => x.Pk_Transactions == new Guid(key));
                var norm = cenResN_Transactions?.FirstOrDefault(x => x.Pk_Transactions == new Guid(key));

                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "TransactionSource",
                    EInDbValue = ein?.TransactionSource,
                    CenResDbValue = cen?.TransactionSource,
                    MongoDbValue = mongo?.TransactionSource,
                    CenResNormalizeDbValue = norm?.TransactionSource
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "TransactionGroup",
                    EInDbValue = ein?.TransactionGroup,
                    CenResDbValue = cen?.TransactionGroup,
                    MongoDbValue = "NoValue",
                    CenResNormalizeDbValue = norm?.TransactionGroup
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "TransactionDate",
                    EInDbValue = ein?.TransactionDate.ToString(),
                    CenResDbValue = cen?.TransactionDate.ToString(),
                    MongoDbValue = mongo?.TransactionDate.ToString(),
                    CenResNormalizeDbValue = norm?.TransactionDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "TransactionCode",
                    EInDbValue = ein?.TransactionCode,
                    CenResDbValue = cen?.TransactionCode,
                    MongoDbValue = mongo?.TransactionCode,
                    CenResNormalizeDbValue = norm?.TransactionCode
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "CurrencyCode",
                    EInDbValue = ein?.CurrencyCode,
                    CenResDbValue = cen?.CurrencyCode,
                    MongoDbValue = mongo?.CurrencyCode,
                    CenResNormalizeDbValue = norm?.CurrencyCode
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "CreditAmount",
                    EInDbValue = ein?.CreditAmount,
                    CenResDbValue = cen?.CreditAmount,
                    MongoDbValue = mongo?.CreditAmount,
                    CenResNormalizeDbValue = norm?.CreditAmount
                });
                comparisonRows.Add(new FieldComparisonRowTransaction
                {
                    PropertyId = cen?.CendynPropertyId,
                    ReservationId = cen?.ExternalResID1,
                    TransactionId = cen?.TransactionId,
                    TransactionDate = cen?.TransactionDate.ToString(),
                    FieldName = "DebitAmount",
                    EInDbValue = ein?.DebitAmount,
                    CenResDbValue = cen?.DebitAmount,
                    MongoDbValue = mongo?.DebitAmount,
                    CenResNormalizeDbValue = norm?.DebitAmount
                });

            }
            for (int i = 0; i < comparisonRows.Count; i++)
            {
                var row = comparisonRows[i];
                ws.Cell(i + 2, 1).Value = row.PropertyId;
                ws.Cell(i + 2, 2).Value = row.ReservationId;
                ws.Cell(i + 2, 3).Value = row.TransactionId;
                ws.Cell(i + 2, 4).Value = row.TransactionDate;
                ws.Cell(i + 2, 5).Value = row.FieldName;
                ws.Cell(i + 2, 6).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(i + 2, 7).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(i + 2, 8).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(i + 2, 9).Value = row?.CenResNormalizeDbValue ?? string.Empty;
            }
            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);
        }

        private static string BuildMongoUserId(CenResProfiles profile)
        {
            return $"{profile.ExternalProfileID}-{profile.CendynPropertyId}-{(string.IsNullOrEmpty(profile.ExternalProfileID2) ? "pms" : profile.ExternalProfileID2)}";
        }

        private static string BuildTransactionCode(CenResReservations reservation)
        {
            return $"{reservation.ExternalResID1}-{reservation.CendynPropertyID}-{(string.IsNullOrEmpty(reservation.ExternalResID2) ? "PMS" : reservation.ExternalResID2)}";
        }

        private string FormatConnectionString(AvailableConnectionInformation conn)
        {
            return $"Server={conn.ServerName};Database={conn.DatabaseName};User Id={conn.DatabaseUser};Password={conn.DatabasePassword};TrustServerCertificate=True;";
        }

        private static (EInDbRepository eInRepo, CenResDbRepository cenResRepo, MongoDbRepository mongoRepo, CenResNormalizeDbRepository cenResNRepo)
        CreateAllRepositories()
        {
            string eIn_connectionString = "Server=QDB-D1001.CENTRALSERVICES.LOCAL;Database=eInsightCRM_Origami_QA;Integrated Security=True;TrustServerCertificate=True;";
            string CenRes_connectionString = "Server=QDB-D1007.CENTRALSERVICES.LOCAL;Database=CenRes_QA_Test;Integrated Security=True;TrustServerCertificate=True;";
            string cenresNormalizeConnStr = "Server=ddbeus2bi01.CENTRALSERVICES.LOCAL;Database=CCRMBIStaging_Normalized_QA;Integrated Security=True;TrustServerCertificate=True;";
            string mongoConnStr = "mongodb+srv://int_skumar:asdj3928ASDk2q*2as@stg-mongo-cluster-01.kk0bg.mongodb.net/";
            string mongoDbName = "push_platform_stg";
            mongoDbName = mongoDbName.Replace("metadata_", "");
            string parentCompanyId = "67371b9bd167a7000161f496"; //Cendyn Account Id // Mongo ParentCompanyId 
            var repo = new EInDbRepository(eIn_connectionString);
            var cenResRepo = new CenResDbRepository(CenRes_connectionString);
            var mongoRepo = new MongoDbRepository(mongoConnStr, mongoDbName, parentCompanyId);
            var cenResNRepo = new CenResNormalizeDbRepository(cenresNormalizeConnStr);
            return (repo, cenResRepo, mongoRepo, cenResNRepo);
        }
        public class FieldComparisonRow
        {
            public string Key { get; set; }
            public string PropertyId { get; set; }
            public string ProfileId { get; set; }
            public string FieldName { get; set; }
            public dynamic? EInDbValue { get; set; }
            public dynamic? CenResDbValue { get; set; }
            public dynamic? MongoDbValue { get; set; }
            public dynamic? CenResNormalizeDbValue { get; set; }
        }
        public class FieldComparisonRowReservations
        {
            public string Key { get; set; }
            public string PropertyId { get; set; }
            public string ReservationId { get; set; }
            public string FieldName { get; set; }
            public dynamic? EInDbValue { get; set; }
            public dynamic? CenResDbValue { get; set; }
            public dynamic? MongoDbValue { get; set; }
            public dynamic? CenResNormalizeDbValue { get; set; }
        }
        public class FieldComparisonRowStayDetail
        {
            public string Key { get; set; }
            public string PropertyId { get; set; }
            public string ReservationId { get; set; }
            public string StayDate { get; set; }
            public string FieldName { get; set; }
            public dynamic? EInDbValue { get; set; }
            public dynamic? CenResDbValue { get; set; }
            public dynamic? MongoDbValue { get; set; }
            public dynamic? CenResNormalizeDbValue { get; set; }
        }
        public class FieldComparisonRowTransaction
        {
            public string PropertyId { get; set; }
            public string ReservationId { get; set; }
            public string TransactionId { get; set; }
            public string TransactionDate { get; set; }
            public string FieldName { get; set; }
            public dynamic? EInDbValue { get; set; }
            public dynamic? CenResDbValue { get; set; }
            public dynamic? MongoDbValue { get; set; }
            public dynamic? CenResNormalizeDbValue { get; set; }
        }
    }
}