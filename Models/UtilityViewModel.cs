namespace CendynDataComparisonUtility.Models
{
    public class UtilityViewModel
    {
        public string SearchString { get; set; }
        public List<DatabaseInfo> DatabaseInfo { get; set; } 
        public List<AvailableConnectionInformation> ConnectionInformation { get; set; }
    }

    public class AvailableConnectionInformation()
    {
        public string ParentCompanyId { get; set; }
        public string CompanyName { get; set; }
        public string ServerName { get; set; }
        public string DatabaseName { get; set; }
        public string DatabaseUser { get; set; }
        public string DatabasePassword { get; set; }
        public string DatabaseCType { get; set; }
        public string Environment { get; set; }
        public string ConnectionString { get; set; }
    }
    public class DatabaseInfo
    {
        public string DbType { get; set; }      // e.g., "eInsightAppDb" or "CenResNormalizeDb"
        public string Environment { get; set; } // e.g., "Dev" or "QA"
        public string Name { get; set; }        // e.g., "eInsightAppDb" or "CenResNormalizeDb"
        public string ServerName { get; set; }
    }

}
