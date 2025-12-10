using CendynDataComparisonUtility.Data;
using CendynDataComparisonUtility.Models;
using CendynDataComparisonUtility.Models.CenResDb;
using CendynDataComparisonUtility.Models.ClientDb;
using CendynDataComparisonUtility.Models.Dtos;
using CendynDataComparisonUtility.Service;
using ClosedXML.Excel;
using Dapper;
using Humanizer;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Data.SqlClient;
using MongoDB.Driver;
using Newtonsoft.Json;
using static CendynDataComparisonUtility.Data.MongoDbRepository;
using static CendynDataComparisonUtility.Service.CenResNormalizeDbRepository;
using static CendynDataComparisonUtility.Utility.QueryDefinitions;

namespace CendynDataComparisonUtility.Controllers
{
    public class UtilityController : Controller
    {
        private readonly IConfiguration _config;
        readonly string[] dbsList = ["eInAppDb", "CenResDb", "MongoDb", "CenResNormalizeDb"];
        public List<ConnectionInfo> eInConnection;
        public UtilityController(IConfiguration config)
        {
            _config = config;
        }

        public class ConnectionInfo()
        {
            public string ParentCompanyId { get; set; }
            public string ParentCompanyName { get; set; }
        }
        public IActionResult Index(string searchString)
        {
            List<AvailableConnectionInformation> avlConnection = new();
            // If searchString is provided, update avlConnection and session
            if (!string.IsNullOrEmpty(searchString))
            {
                foreach (var db in dbsList)
                {
                    var section = _config.GetSection(db);
                    foreach (var env in section.GetChildren())
                    {
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
                        if (db == "eInAppDb")
                        {
                            var connStr = env.GetValue<string>("ConnectionString");
                            if (string.IsNullOrEmpty(connStr)) continue;

                            using var connection = new SqlConnection(connStr);
                            if (eInConnection == null)
                            {
                                connection.Open();
                                eInConnection = connection.Query<ConnectionInfo>(
                                    "SELECT Id AS ParentCompanyId, ParentCompany AS ParentCompanyName FROM [dbo].[CendynAdmin_ParentCompany] WITH(NOLOCK) WHERE ParentCompany LIKE @search",
                                    new { search = $"%{searchString}%" }
                                ).ToList();
                            }
                            foreach (var item in eInConnection.ToList())
                            {
                                var sql = @"SELECT TOP 1 Id AS ParentCompanyId, ParentCompany AS CompanyName, CRM_SERVER AS ServerName, CRM_Database AS DatabaseName, CRM_User AS DatabaseUser, CRM_Password AS DatabasePassword, 'EINClientDb' AS DatabaseCType 
                                            FROM V_CRMCONNECTIONS WITH(NOLOCK) WHERE Id=@ParentCompanyId;";

                                var eInsightClientDb = connection.QueryFirstOrDefault<AvailableConnectionInformation>(sql, new { ParentCompanyId = item.ParentCompanyId });
                                if (eInsightClientDb != null)
                                    avlConnection.Add(eInsightClientDb);
                                else
                                    eInConnection.Remove(item);
                            }



                        }
                        if (db == "CenResDb")
                        {
                            var connStr = env.GetValue<string>("ConnectionString");
                            if (string.IsNullOrEmpty(connStr)) continue;
                            foreach (var item in eInConnection)
                            {
                                using var connection = new SqlConnection(connStr);
                                var sql = @"SELECT TOP 1 @ParentCompanyName AS CompanyName, @ParentCompanyId AS ParentCompanyId, dd.machName as ServerName ,dd.dbName as DatabaseName, 'CenResDb' AS DatabaseCType 
                                                    FROM dbo.Locations AS l WITH ( NOLOCK ) 
                                                    LEFT JOIN dbo.Loc_Inst_Map LIM WITH (NOLOCK) ON l.pk_Locations = LIM.fk_Locations 
                                                    LEFT JOIN dbo.Installations AS i WITH ( NOLOCK ) ON i.pk_Installations = LIM.fk_Installations 
                                                    LEFT JOIN DevOpsPortal.dbo.DBCenResByPropertyID AS dd WITH(NOLOCK) ON dd.CendynPropertyID = i.NBB_ID 
                                                    WHERE l.MgmtCompany=@ParentCompanyName";
                                var CenResDb = connection.QueryFirstOrDefault<AvailableConnectionInformation>(sql
                                    , new { ParentCompanyName = item.ParentCompanyName, ParentCompanyId = item.ParentCompanyId });

                                if (CenResDb != null)
                                    avlConnection.Add(CenResDb);
                            }
                        }
                        if (db == "CenResNormalizeDb")
                        {
                            var connStr = env.GetValue<string>("ConnectionString");
                            var connstrBuilder = new SqlConnectionStringBuilder(connStr);
                            if (string.IsNullOrEmpty(connStr)) continue;
                            using var connection = new SqlConnection(connStr);
                            connection.Open();
                            var cenResCompany = connection.Query<ConnectionInfo>(
                            "SELECT ParentCompanyId, ParentCompanyName FROM [CCRM].[ParentCompany] WITH(NOLOCK) WHERE ParentCompanyName LIKE @search",
                            new { search = $"%{searchString}%" }
                        );
                            if (cenResCompany != null)
                            {
                                foreach (var item in cenResCompany)
                                {
                                    avlConnection.Add(new AvailableConnectionInformation()
                                    {
                                        ParentCompanyId = item.ParentCompanyId,
                                        CompanyName = item.ParentCompanyName,
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
                HttpContext.Session.Remove("AvlConnection");
                HttpContext.Session.Remove("LastSearchedCompany");
                // Update session with new search results
                HttpContext.Session.SetString("AvlConnection", JsonConvert.SerializeObject(avlConnection));
                HttpContext.Session.SetString("LastSearchedCompany", searchString ?? string.Empty);
            }
            else
            {
                var avlConnectionJson = HttpContext.Session.GetString("AvlConnection");
                if (!string.IsNullOrEmpty(avlConnectionJson))
                    avlConnection = JsonConvert.DeserializeObject<List<AvailableConnectionInformation>>(avlConnectionJson);
                else
                    avlConnection = new List<AvailableConnectionInformation>();
            }
            var viewModel = new UtilityViewModel()
            {
                SearchString = string.IsNullOrEmpty(searchString) ? string.Empty : searchString,
                DatabaseInfo = GetConfiguredDatabases(),
                ConnectionInformation = avlConnection
            };
            return View(viewModel);
        }

        public List<DatabaseInfo> GetConfiguredDatabases()
        {
            List<DatabaseInfo> Databases = new();
            foreach (var db in dbsList)
            {
                var section = _config.GetSection(db);
                foreach (var env in section.GetChildren())
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
            }
            return Databases;
        }


        [HttpGet("Utility/VolumeBasedResult")]
        public FileContentResult VolumeBasedResult()
        {
            // EInDb counts
            //string einConnStr = "Server=QDB-D1001.CENTRALSERVICES.LOCAL;Database=eInsightCRM_Origami_QA;Integrated Security=True;TrustServerCertificate=True;";
            //var einRepo = new EInDbRepository(einConnStr);
            //var einRows = einRepo.GetEInDbCountRows();
            var einRows = new List<DbCountRow>(); // Temporarily disable EInDb counts

            // CenResDb counts
            string cenResConnStr = "Server=QDB-D1007.CENTRALSERVICES.LOCAL;Database=CenRes_QA_Test;Integrated Security=True;TrustServerCertificate=True;";
            var cenResRepo = new CenResDbRepository(cenResConnStr);
            var cenResRows = cenResRepo.GetCenResDbCountRows("1054");
            var cendynPropertyIds = cenResRows.Select(r => r.CendynPropertyId).Distinct().ToList();

            // MongoDb counts
            string mongoConnStr = "mongodb+srv://int_skumar:asdj3928ASDk2q*2as@stg-mongo-cluster-01.kk0bg.mongodb.net/";
            string mongoDbName = "push_platform_stg";
            string parentCompanyId = "67371b9bd167a7000161f496";

            var mongoRepo = new MongoDbRepository(mongoConnStr, mongoDbName, parentCompanyId);
            var mongoRows = mongoRepo.GetMongoDbCountRows(cendynPropertyIds);

            var mongoPropertyId = mongoRows
                                .GroupBy(x => x.CendynPropertyId)
                                .Select(g => new CendynPropertyMongoHotelIdMapping
                                {
                                    CendynPropertyId = g.Key,
                                    MongoPropertyId = g.Select(x => x.MongoHotelId).FirstOrDefault()
                                })
                                .ToList();
            // CenResNormalizeDb counts
            string normConnStr = "Server=ddbeus2bi01.CENTRALSERVICES.LOCAL;Database=CCRMBIStaging_Normalized_QA;Integrated Security=True;TrustServerCertificate=True;";
            var normRepo = new CenResNormalizeDbRepository(normConnStr);
            var normRows = normRepo.GetCenResNormalizeDbCountRows(mongoPropertyId);

            using var wb = new XLWorkbook();
            var ws = wb.Worksheets.Add("VolumeBasedResult");

            // Header
            ws.Cell(1, 1).Value = "Property ID";
            ws.Cell(1, 2).Value = "Range";
            ws.Cell(1, 3).Value = "Table Name";
            ws.Cell(1, 4).Value = "eInsight";
            ws.Cell(1, 5).Value = "CenRes";
            ws.Cell(1, 6).Value = "MongoDb";
            ws.Cell(1, 7).Value = "CenResNormalize";

            // Group by PropertyId, Range, TableName
            var allRows = cenResRows
                .Concat(einRows)
                .Concat(normRows)
                .Concat(mongoRows)
                .GroupBy(r => new { r.CendynPropertyId, r.Range, r.TableName })
                .Select(g => new
                {
                    PropertyId = g.Key.CendynPropertyId,
                    Range = g.Key.Range,
                    TableName = g.Key.TableName,
                    EInDb = einRows?.FirstOrDefault(x => x.CendynPropertyId == g.Key.CendynPropertyId && x.Range == g.Key.Range && x.TableName == g.Key.TableName)?.Count ?? 0,
                    CenResDb = cenResRows.FirstOrDefault(x => x.CendynPropertyId == g.Key.CendynPropertyId && x.Range == g.Key.Range && x.TableName == g.Key.TableName)?.Count ?? 0,
                    MongoDb = mongoRows.FirstOrDefault(x => x.CendynPropertyId == g.Key.CendynPropertyId && x.Range == g.Key.Range && x.TableName == g.Key.TableName)?.Count ?? 0,
                    CenResNormalizeDb = normRows.FirstOrDefault(x => x.CendynPropertyId == g.Key.CendynPropertyId && x.Range == g.Key.Range && x.TableName == g.Key.TableName)?.Count ?? 0
                })
                .OrderBy(x => x.PropertyId)
                .ThenBy(x => x.TableName)
                .ThenBy(x => x.Range)
                .ToList();

            int row = 2;
            foreach (var item in allRows)
            {
                ws.Cell(row, 1).Value = item.PropertyId;
                ws.Cell(row, 2).Value = item.Range;
                ws.Cell(row, 3).Value = item.TableName;
                ws.Cell(row, 4).Value = item.EInDb;
                ws.Cell(row, 5).Value = item.CenResDb;
                ws.Cell(row, 6).Value = item.MongoDb;
                ws.Cell(row, 7).Value = item.CenResNormalizeDb;
                row++;
            }

            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);

            using var stream = new MemoryStream();
            wb.SaveAs(stream);
            stream.Position = 0;
            return File(stream.ToArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "VolumeBasedResult.xlsx");
        }

        /// <summary>
        /// Compare Data from EInDb >> CenResDb >> MongoDb >> NormalizeDb
        /// </summary>
        /// <returns></returns> 
        [HttpGet("Utility/CompareData")]
        public FileContentResult CompareData([FromQuery] int featureSet = 1, [FromQuery] string searchString = null)
        {
            using var wb = new XLWorkbook();
            var ws = wb.Worksheets.Add("TOP 100 Records");
            ws.Cell(1, 1).Value = "PropertyId";
            ws.Cell(1, 2).Value = "ExternalId1";
            ws.Cell(1, 3).Value = "ExternalId2/TransactionId";
            ws.Cell(1, 4).Value = "Stay Date/Transaction Date";
            ws.Cell(1, 5).Value = "Table Name";
            ws.Cell(1, 6).Value = "Field Name";
            ws.Cell(1, 7).Value = "eInDb";
            ws.Cell(1, 8).Value = "CenResDb";
            ws.Cell(1, 9).Value = "MongoDb";
            ws.Cell(1, 10).Value = "CenResNormalizeDb";

            int rowIndex = 2;

            // Profiles
            var profilesData = CreateProfilesSheet(featureSet);
            foreach (var row in profilesData)
            {
                ws.Cell(rowIndex, 1).Value = row.PropertyId;
                ws.Cell(rowIndex, 2).Value = row.ExternalId1;
                ws.Cell(rowIndex, 3).Value = row.ExternalId2OrTransactionId;
                ws.Cell(rowIndex, 4).Value = ""; // No Stay Date for Profiles
                ws.Cell(rowIndex, 5).Value = row.TableName;
                ws.Cell(rowIndex, 6).Value = row.FieldName;
                ws.Cell(rowIndex, 7).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(rowIndex, 8).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(rowIndex, 9).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(rowIndex, 10).Value = row?.CenResNormalizeDbValue ?? string.Empty;
                rowIndex++;
            }

            // Reservations
            var reservationsData = CreateReservationsSheet(featureSet);
            foreach (var row in reservationsData)
            {
                ws.Cell(rowIndex, 1).Value = row.PropertyId;
                ws.Cell(rowIndex, 2).Value = row.ExternalId1;
                ws.Cell(rowIndex, 3).Value = row.ExternalId2OrTransactionId;
                ws.Cell(rowIndex, 4).Value = row.StayDateOrTransactionDate?.ToString() ?? "";
                ws.Cell(rowIndex, 5).Value = row.TableName;
                ws.Cell(rowIndex, 6).Value = row.FieldName;
                ws.Cell(rowIndex, 7).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(rowIndex, 8).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(rowIndex, 9).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(rowIndex, 10).Value = row?.CenResNormalizeDbValue ?? string.Empty;
                rowIndex++;
            }

            // StayDetails
            var stayDetailsData = CreateStayDetailsSheet(featureSet);
            foreach (var row in stayDetailsData)
            {
                ws.Cell(rowIndex, 1).Value = row.PropertyId;
                ws.Cell(rowIndex, 2).Value = row.ExternalId1;
                ws.Cell(rowIndex, 3).Value = row.ExternalId2OrTransactionId;
                ws.Cell(rowIndex, 4).Value = row.StayDateOrTransactionDate?.ToString("yyyy-MM-dd HH:mm:ss") ?? "";
                ws.Cell(rowIndex, 5).Value = row.TableName;
                ws.Cell(rowIndex, 6).Value = row.FieldName;
                ws.Cell(rowIndex, 7).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(rowIndex, 8).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(rowIndex, 9).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(rowIndex, 10).Value = row.StayDateOrTransactionDate?.ToString("yyyy-MM-dd HH:mm:ss") ?? "";
                rowIndex++;
            }

            // Transactions
            var transactionsData = CreateTransactionsSheet(featureSet);
            foreach (var row in transactionsData)
            {
                ws.Cell(rowIndex, 1).Value = row.PropertyId;
                ws.Cell(rowIndex, 2).Value = row.ExternalId1;
                ws.Cell(rowIndex, 3).Value = row.ExternalId2OrTransactionId;
                ws.Cell(rowIndex, 4).Value = row.StayDateOrTransactionDate?.ToString() ?? "";
                ws.Cell(rowIndex, 5).Value = row.TableName;
                ws.Cell(rowIndex, 6).Value = row.FieldName;
                ws.Cell(rowIndex, 7).Value = row?.EInDbValue ?? string.Empty;
                ws.Cell(rowIndex, 8).Value = row?.CenResDbValue ?? string.Empty;
                ws.Cell(rowIndex, 9).Value = row?.MongoDbValue ?? string.Empty;
                ws.Cell(rowIndex, 10).Value = row?.CenResNormalizeDbValue ?? string.Empty;
                rowIndex++;
            }

            ws.Columns().AdjustToContents();
            ws.SheetView.FreezeRows(1);
            using var stream = new MemoryStream();
            wb.SaveAs(stream);
            stream.Position = 0;
            return File(stream.ToArray(), "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", "DataComparison.xlsx");
        }

        private List<FieldComparisonRow> CreateProfilesSheet(int featureSet)
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
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "First Name",
                    EInDbValue = ein?.FirstName,
                    CenResDbValue = cen?.FirstName,
                    MongoDbValue = mongo?.FirstName,
                    CenResNormalizeDbValue = norm?.FirstName
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Last Name",
                    EInDbValue = ein?.LastName,
                    CenResDbValue = cen?.LastName,
                    MongoDbValue = mongo?.LastName,
                    CenResNormalizeDbValue = norm?.LastName
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Salutation",
                    EInDbValue = ein?.Salutation,
                    CenResDbValue = cen?.Salutation,
                    MongoDbValue = mongo?.Salutation,
                    CenResNormalizeDbValue = norm?.Salutation
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Address1",
                    EInDbValue = ein?.Address1,
                    CenResDbValue = cen?.Address1,
                    MongoDbValue = mongo?.Address1,
                    CenResNormalizeDbValue = norm?.Address1
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "City",
                    EInDbValue = ein?.City,
                    CenResDbValue = cen?.City,
                    MongoDbValue = mongo?.City,
                    CenResNormalizeDbValue = norm?.City
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "StateProvinceCode",
                    EInDbValue = ein?.StateProvinceCode,
                    CenResDbValue = cen?.StateProvince,
                    MongoDbValue = "No value",
                    CenResNormalizeDbValue = norm?.StateProvince
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "ZipCode",
                    EInDbValue = ein?.ZipCode,
                    CenResDbValue = cen?.PostalCode,
                    MongoDbValue = mongo?.PostalCode,
                    CenResNormalizeDbValue = norm?.ZipCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "CountryCode",
                    EInDbValue = ein?.CountryCode,
                    CenResDbValue = cen?.CountryCode,
                    MongoDbValue = mongo?.CountryCode,
                    CenResNormalizeDbValue = norm?.CountryCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "PhoneNumber",
                    EInDbValue = ein?.PhoneNumber,
                    CenResDbValue = cen?.PhoneNumber,
                    MongoDbValue = mongo?.PhoneNumber,
                    CenResNormalizeDbValue = norm?.PhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "HomePhoneNumber",
                    EInDbValue = ein?.HomePhoneNumber,
                    CenResDbValue = cen?.HomePhone,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.HomePhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "FaxNumber",
                    EInDbValue = ein?.FaxNumber,
                    CenResDbValue = cen?.FaxNumber,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.FaxNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Email",
                    EInDbValue = ein?.Email,
                    CenResDbValue = cen?.Email,
                    MongoDbValue = mongo?.Email,
                    CenResNormalizeDbValue = norm?.Email
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Languages",
                    EInDbValue = ein?.Languages,
                    CenResDbValue = cen?.Language,
                    MongoDbValue = mongo?.Language,
                    CenResNormalizeDbValue = norm?.Languages
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "Nationality",
                    EInDbValue = ein?.Nationality,
                    CenResDbValue = cen?.Nationality,
                    MongoDbValue = mongo?.Nationality,
                    CenResNormalizeDbValue = norm?.Nationality
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "CellPhoneNumber",
                    EInDbValue = ein?.CellPhoneNumber,
                    CenResDbValue = "No Value",
                    MongoDbValue = "No value",
                    CenResNormalizeDbValue = norm?.CellPhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "BusinessPhoneNumber",
                    EInDbValue = ein?.BusinessPhoneNumber,
                    CenResDbValue = cen?.WorkPhone,
                    MongoDbValue = "No Value",
                    CenResNormalizeDbValue = norm?.BusinessPhoneNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "CompanyTitle",
                    EInDbValue = ein?.CompanyTitle,
                    CenResDbValue = cen?.CompanyName,
                    MongoDbValue = mongo?.CompanyName,
                    CenResNormalizeDbValue = norm?.CompanyTitle
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "JobTitle",
                    EInDbValue = ein?.JobTitle,
                    CenResDbValue = cen?.JobTitle,
                    MongoDbValue = mongo?.JobTitle,
                    CenResNormalizeDbValue = norm?.JobTitle
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "AllowEMail",
                    EInDbValue = ein?.AllowEMail,
                    CenResDbValue = cen?.AllowEmail,
                    MongoDbValue = mongo?.AllowEmail,
                    CenResNormalizeDbValue = norm?.AllowEMail
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ExternalProfileID,
                    ExternalId2OrTransactionId = cen.ExternalProfileID2,
                    TableName = "Profiles",
                    FieldName = "AllowMail",
                    EInDbValue = ein?.AllowMail,
                    CenResDbValue = cen?.AllowMail,
                    MongoDbValue = mongo?.AllowMail,
                    CenResNormalizeDbValue = norm?.AllowMail
                });
            }
            return comparisonRows;
        }
        private List<FieldComparisonRow> CreateReservationsSheet(int featureSet)
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

            var comparisonRows = new List<FieldComparisonRow>();
            foreach (var key in allKeys) // union of all PKs from all sources
            {
                var ein = eIn_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);
                var cen = cenRes_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);
                var mongo = mongo_Reservations?.FirstOrDefault(x => x.UniqId_ExternalResID1 == key);
                var norm = cenResN_Reservations?.FirstOrDefault(x => x.ReservationNumber == key);

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "Sub Reservation Number",
                    EInDbValue = ein?.SubReservationNumber,
                    CenResDbValue = cen?.SubReservationNumber,
                    MongoDbValue = mongo?.ConfirmationNumber,
                    CenResNormalizeDbValue = norm?.SubReservationNumber
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "CentralReservation",
                    EInDbValue = ein?.CentralReservation,
                    CenResDbValue = cen?.CentralReservation,
                    MongoDbValue = mongo?.CentralResNum,
                    CenResNormalizeDbValue = norm?.CentralReservation
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "BookingEngConfNum",
                    EInDbValue = ein?.BookingEngConfNum,
                    CenResDbValue = cen?.BookingEngConfNum,
                    MongoDbValue = mongo?.BookingSourceName,
                    CenResNormalizeDbValue = norm?.SourceOfBusiness
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "StayStatus",
                    EInDbValue = ein?.StayStatus,
                    CenResDbValue = cen?.StayStatus,
                    MongoDbValue = mongo?.ResStatusCode,
                    CenResNormalizeDbValue = norm?.StayStatus
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "ArrivalDate",
                    EInDbValue = ein?.ArrivalDate.ToString(),
                    CenResDbValue = cen?.ArrivalDate.ToString(),
                    MongoDbValue = mongo?.ResArriveDate.ToString(),
                    CenResNormalizeDbValue = norm?.ArrivalDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "DepartureDate",
                    EInDbValue = ein?.DepartureDate.ToString(),
                    CenResDbValue = cen?.DepartureDate.ToString(),
                    MongoDbValue = mongo?.ResDepartDate.ToString(),
                    CenResNormalizeDbValue = norm?.DepartureDate.ToString()
                });

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "BookingDate",
                    EInDbValue = ein?.BookingDate.ToString(),
                    CenResDbValue = cen?.BookingDate?.ToString(),
                    MongoDbValue = mongo?.BookingDate?.ToString(),
                    CenResNormalizeDbValue = norm?.BookingDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "CancelDate",
                    EInDbValue = ein?.CancelDate.ToString(),
                    CenResDbValue = cen?.CancelDate?.ToString(),
                    MongoDbValue = mongo?.CancelDate?.ToString(),
                    CenResNormalizeDbValue = norm?.CancelDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "GroupReservation",
                    EInDbValue = ein?.GroupReservation?.ToString(), // Use null-conditional operator
                    CenResDbValue = cen?.GroupReservation?.ToString(), // Use null-conditional operator
                    MongoDbValue = mongo?.GroupName?.ToString(), // Use null-conditional operator
                    CenResNormalizeDbValue = norm?.GroupReservation?.ToString() // Use null-conditional operator
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "Channel",
                    EInDbValue = ein?.Channel?.ToString(),
                    CenResDbValue = cen?.Channel?.ToString(),
                    MongoDbValue = mongo?.StayChannelCode?.ToString(),
                    CenResNormalizeDbValue = norm?.Channel?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "SourceOfBusiness",
                    EInDbValue = ein?.SourceOfBusiness?.ToString(),
                    CenResDbValue = cen?.SourceOfBusiness?.ToString(),
                    MongoDbValue = mongo?.BookingSourceName,
                    CenResNormalizeDbValue = norm?.SourceOfBusiness?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "MarketSeg",
                    EInDbValue = ein?.MarketSeg?.ToString(),
                    CenResDbValue = cen?.MarketSeg?.ToString(),
                    MongoDbValue = mongo?.MarketSegmentCode,
                    CenResNormalizeDbValue = norm?.MarketSeg?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "MarketSubSeg",
                    EInDbValue = ein?.MarketSubSeg?.ToString(),
                    CenResDbValue = cen?.MarketSubSeg?.ToString(),
                    MongoDbValue = "",// mongo field not available
                    CenResNormalizeDbValue = norm?.MarketSubSeg?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "RoomNights",
                    EInDbValue = ein?.RoomNights.ToString(),
                    CenResDbValue = cen?.RoomNights.ToString(),
                    MongoDbValue = mongo?.NumOfNights.ToString(),
                    CenResNormalizeDbValue = norm?.RoomNights.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "NumAdults",
                    EInDbValue = ein?.NumAdults.ToString(),
                    CenResDbValue = cen?.NumAdults.ToString(),
                    MongoDbValue = mongo?.NumOfAdults.ToString(),
                    CenResNormalizeDbValue = norm?.NumAdults.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "NumChildren",
                    EInDbValue = ein?.NumChildren.ToString(),
                    CenResDbValue = cen?.NumChildren.ToString(),
                    MongoDbValue = mongo?.NumOfChildren.ToString(),
                    CenResNormalizeDbValue = norm?.NumChildren.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "TotalPersons",
                    EInDbValue = ein?.TotalPersons.ToString(),
                    CenResDbValue = cen?.TotalPersons.ToString(),
                    MongoDbValue = mongo?.TotalPersons.ToString(),
                    CenResNormalizeDbValue = norm?.TotalPersons.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "RateType",
                    EInDbValue = ein?.RateType?.ToString(),
                    CenResDbValue = cen?.RateType?.ToString(),
                    MongoDbValue = mongo?.RateType?.ToString(),
                    CenResNormalizeDbValue = norm?.RateType?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "RoomTypeCode",
                    EInDbValue = ein?.RoomTypeCode?.ToString(),
                    CenResDbValue = cen?.RoomTypeCode?.ToString(),
                    MongoDbValue = mongo?.RoomTypeCode?.ToString(),
                    CenResNormalizeDbValue = norm?.RoomTypeCode?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "RoomCode",
                    EInDbValue = ein?.RoomCode?.ToString(),
                    CenResDbValue = cen?.RoomCode?.ToString(),
                    MongoDbValue = mongo?.RoomCode?.ToString(),
                    CenResNormalizeDbValue = norm?.RoomCode?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "IATA",
                    EInDbValue = ein?.IATA?.ToString(),
                    CenResDbValue = cen?.IATA?.ToString(),
                    MongoDbValue = mongo?.TravelAgentIata?.ToString(),
                    CenResNormalizeDbValue = norm?.IATA?.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "NumRooms",
                    EInDbValue = ein?.NumRooms.ToString(),
                    CenResDbValue = cen?.NumRooms.ToString(),
                    MongoDbValue = mongo?.NumberRooms.ToString(),
                    CenResNormalizeDbValue = norm?.NumRooms.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "RoomRevenue",
                    EInDbValue = ein?.RoomRevenue.ToString(),
                    CenResDbValue = cen?.RoomRevenue.ToString(),
                    MongoDbValue = mongo?.RoomRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.RoomRevenue.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "Tax",
                    EInDbValue = ein?.Tax.ToString(),
                    CenResDbValue = cen?.Tax.ToString(),
                    MongoDbValue = mongo?.TotalTax.ToString(),
                    CenResNormalizeDbValue = norm?.Tax.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "OtherRevenues",
                    EInDbValue = ein?.OtherRevenue.ToString(),
                    CenResDbValue = cen?.OtherRevenue.ToString(),
                    MongoDbValue = mongo?.TotalOtherRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.OtherRevenue.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyID,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = cen.ExternalResID2,
                    StayDateOrTransactionDate = cen?.ArrivalDate,
                    TableName = "Reservations",
                    FieldName = "TotalRevenue",
                    EInDbValue = ein?.TotalRevenue.ToString(),
                    CenResDbValue = cen?.TotalRevenue.ToString(),
                    MongoDbValue = mongo?.TotalRevenue.ToString(),
                    CenResNormalizeDbValue = norm?.TotalRevenue.ToString()
                });
            }

            return comparisonRows;
        }
        private List<FieldComparisonRow> CreateStayDetailsSheet(int featureSet)
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

            var comparisonRows = new List<FieldComparisonRow>();
            foreach (var key in allKeys)
            {
                var ein = eIn_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);
                var cen = cenRes_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);
                var mongo = mongo_StayDetails?.FirstOrDefault(x => x.UniqId_ExternalResID1 == key);
                var norm = cenResN_StayDetails?.FirstOrDefault(x => x.ReservationNumber == key);

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "StayDate",
                    EInDbValue = ein?.StayDate,
                    CenResDbValue = cen?.StayDate,
                    MongoDbValue = mongo?.StayDate,
                    CenResNormalizeDbValue = norm?.StayDate
                });

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "StayRateType",
                    EInDbValue = ein?.StayRateType,
                    CenResDbValue = cen?.StayRateType,
                    MongoDbValue = mongo?.RateType,
                    CenResNormalizeDbValue = norm?.StayRateType
                });

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "StayRoomType",
                    EInDbValue = ein?.StayRoomType,
                    CenResDbValue = cen?.StayRoomType,
                    MongoDbValue = mongo?.RoomType,
                    CenResNormalizeDbValue = norm?.StayRoomType
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "StayRateAmount",
                    EInDbValue = ein?.StayRateAmount,
                    CenResDbValue = cen?.StayRateAmount,
                    MongoDbValue = mongo?.StayRateAmount,
                    CenResNormalizeDbValue = norm?.StayRateAmount
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "StayNumRooms",
                    EInDbValue = ein?.StayNumRooms,
                    CenResDbValue = cen?.NumberOfRooms,
                    MongoDbValue = mongo?.NumberOfRooms,
                    CenResNormalizeDbValue = norm?.StayNumRooms
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen.ReservationNumber,
                    ExternalId2OrTransactionId = "", //check this field
                    StayDateOrTransactionDate = cen?.StayDate,
                    TableName = "StayDetails",
                    FieldName = "CurrencyCode",
                    EInDbValue = ein?.CurrencyCode,
                    CenResDbValue = cen?.CurrencyCode,
                    MongoDbValue = mongo?.CurrencyCode,
                    CenResNormalizeDbValue = norm?.CurrencyCode
                });
            }
            return comparisonRows;
        }
        private List<FieldComparisonRow> CreateTransactionsSheet(int featureSet)
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

            var comparisonRows = new List<FieldComparisonRow>();
            foreach (var key in allKeys) // union of all PKs from all sources
            {
                var ein = eIn_Transaction?.FirstOrDefault(x => x.PK_Transactions == new Guid(key));
                var cen = cenRes_Transactions?.FirstOrDefault(x => x.PK_Transactions == new Guid(key));
                var mongo = mongo_Transactions?.FirstOrDefault(x => x.Pk_Transactions == new Guid(key));
                var norm = cenResN_Transactions?.FirstOrDefault(x => x.Pk_Transactions == new Guid(key));

                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "TransactionSource",
                    EInDbValue = ein?.TransactionSource,
                    CenResDbValue = cen?.TransactionSource,
                    MongoDbValue = mongo?.TransactionSource,
                    CenResNormalizeDbValue = norm?.TransactionSource
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "TransactionGroup",
                    EInDbValue = ein?.TransactionGroup,
                    CenResDbValue = cen?.TransactionGroup,
                    MongoDbValue = "NoValue",
                    CenResNormalizeDbValue = norm?.TransactionGroup
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "TransactionDate",
                    EInDbValue = ein?.TransactionDate.ToString(),
                    CenResDbValue = cen?.TransactionDate.ToString(),
                    MongoDbValue = mongo?.TransactionDate.ToString(),
                    CenResNormalizeDbValue = norm?.TransactionDate.ToString()
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "TransactionCode",
                    EInDbValue = ein?.TransactionCode,
                    CenResDbValue = cen?.TransactionCode,
                    MongoDbValue = mongo?.TransactionCode,
                    CenResNormalizeDbValue = norm?.TransactionCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "CurrencyCode",
                    EInDbValue = ein?.CurrencyCode,
                    CenResDbValue = cen?.CurrencyCode,
                    MongoDbValue = mongo?.CurrencyCode,
                    CenResNormalizeDbValue = norm?.CurrencyCode
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "CreditAmount",
                    EInDbValue = ein?.CreditAmount,
                    CenResDbValue = cen?.CreditAmount,
                    MongoDbValue = mongo?.CreditAmount,
                    CenResNormalizeDbValue = norm?.CreditAmount
                });
                comparisonRows.Add(new FieldComparisonRow
                {
                    PropertyId = cen?.CendynPropertyId,
                    ExternalId1 = cen?.ExternalResID1,
                    ExternalId2OrTransactionId = cen?.TransactionId,
                    StayDateOrTransactionDate = cen?.TransactionDate,
                    TableName = "Transactions",
                    FieldName = "DebitAmount",
                    EInDbValue = ein?.DebitAmount,
                    CenResDbValue = cen?.DebitAmount,
                    MongoDbValue = mongo?.DebitAmount,
                    CenResNormalizeDbValue = norm?.DebitAmount
                });

            }
            return comparisonRows;
        }

        private static string BuildMongoUserId(CenResProfiles profile)
        {
            return $"{profile.ExternalProfileID}-{profile.CendynPropertyId}-{(string.IsNullOrEmpty(profile.ExternalProfileID2) ? "pms" : profile.ExternalProfileID2)}";
        }
        private string FormatConnectionString(AvailableConnectionInformation conn)
        {
            return $"Server={conn.ServerName};Database={conn.DatabaseName};User Id={conn.DatabaseUser};Password={conn.DatabasePassword};TrustServerCertificate=True;";
        }
        private string FormatCenResDbConnectionString(AvailableConnectionInformation conn)
        {
            return $"Server={conn.ServerName};Database={conn.DatabaseName};Integrated Security=True;TrustServerCertificate=True;";
        }


        private (EInDbRepository eInRepo, CenResDbRepository cenResRepo, MongoDbRepository mongoRepo, CenResNormalizeDbRepository cenResNRepo)
        CreateAllRepositories()
        {
            //Get Connection Details from Session
            var (eIn_ConnectionString, cenRes_ConnectionString, mongo_ConnectionString, mongoDbName, mongoParentCompanyId, cenResN_ConnectionString) = GetConnectionFromSession();
            var repo = new EInDbRepository(eIn_ConnectionString);
            var cenResRepo = new CenResDbRepository(cenRes_ConnectionString);
            var mongoRepo = new MongoDbRepository(mongo_ConnectionString, mongoDbName, mongoParentCompanyId);
            var cenResNRepo = new CenResNormalizeDbRepository(cenResN_ConnectionString);
            return (repo, cenResRepo, mongoRepo, cenResNRepo);
        }
        public class FieldComparisonRow
        {
            public string PropertyId { get; set; }
            public string ExternalId1 { get; set; }
            public string ExternalId2OrTransactionId { get; set; }
            public DateTime? StayDateOrTransactionDate { get; set; }
            public string TableName { get; set; }
            public string FieldName { get; set; }
            public dynamic EInDbValue { get; set; }
            public dynamic CenResDbValue { get; set; }
            public dynamic MongoDbValue { get; set; }
            public dynamic CenResNormalizeDbValue { get; set; }
        }

        private (string eIn_ConnectionString, string cenRes_ConnectionString, string mongo_ConnectionString, string mongoDbName, string mongoParentCompanyId, string cenResN_ConnectionString) GetConnectionFromSession()
        {
            var avlConnection = new List<AvailableConnectionInformation>();
            var avlConnectionJson = HttpContext.Session.GetString("AvlConnection");
            if (!string.IsNullOrEmpty(avlConnectionJson))
                avlConnection = JsonConvert.DeserializeObject<List<AvailableConnectionInformation>>(avlConnectionJson);

            var eIn = avlConnection.FirstOrDefault(x => x.DatabaseCType == "EINClientDb");
            var cenRes = avlConnection.FirstOrDefault(x => x.DatabaseCType == "CenResDb");
            var mongo = avlConnection.FirstOrDefault(x => x.DatabaseCType == "MongoDb");
            string mongoDbName = mongo != null && !string.IsNullOrEmpty(mongo.DatabaseName) ? mongo.DatabaseName.Replace("metadata_", "") : null;
            if (mongo != null) mongo.DatabaseName = mongoDbName;
            var cenResN = avlConnection.FirstOrDefault(x => x.DatabaseCType == "CenResNormalizeDb");

            string eInConnStr = eIn != null ? FormatConnectionString(eIn) : null;
            string cenResConnStr = cenRes != null ? FormatCenResDbConnectionString(cenRes) : null;
            string cenResNConnStr = cenResN != null ? cenResN.ConnectionString : null;

            return (eInConnStr, cenResConnStr, mongo.ConnectionString, mongoDbName, mongo.ParentCompanyId, cenResNConnStr);
        }
    }
}