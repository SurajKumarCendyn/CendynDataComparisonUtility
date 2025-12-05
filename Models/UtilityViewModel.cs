using Microsoft.AspNetCore.Mvc.Rendering;

namespace CendynDataComparisonUtility.Models
{
    public class UtilityViewModel
    {
        public string SearchString { get; set; }
        public List<DatabaseInfo> DatabaseInfo { get; set; }
        public SelectList AvailableDataCompareList { get; set; } = new SelectList(new List<string>
        {
            "eInsight <> CenRes <> Mongo DB <> Normalized DB ",
            "CenRes <> Mongo DB <> Normalized DB",
            "NextGuest <> CenRes <> Mongo DB <> Normalized DB ",
            "NextGuest <> Mongo DB <> Normalized DB "
        });
        public List<AvailableConnectionInformation> ConnectionInformation { get; set; }
        public RecordSelectionModel RecordSelectionModel { get; set; } = new RecordSelectionModel();
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

    public class RecordSelectionModel()
    {
        public string SearchString { get; set; }
        public int Feature { get; set; }
        public RecordType RecordType { get; set; } = RecordType.Top100;
        public TimeFrame TimeFrame { get; set; } = TimeFrame.Last3Years;

    }
    public enum RecordType
    {
        Top100,
        Random100
    }
    public enum TimeFrame
    {
        Last3Years,
        AllData
    }

}
