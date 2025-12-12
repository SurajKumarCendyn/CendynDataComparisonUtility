namespace CendynDataComparisonUtility.Models.CenResDb
{
    public class CenResProfiles
    {
        public Guid PK_Profiles { get; set; } 
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Salutation { get; set; }
        public string Address1 { get; set; }
        public string City { get; set; }
        public string StateProvince { get; set; }
        public string PostalCode { get; set; }
        public string CountryCode { get; set; }
        public string PhoneNumber { get; set; }
        public string HomePhone { get; set; }
        public string WorkPhone { get; set; }
        public string FaxNumber { get; set; }
        public string Email { get; set; }
        public string Nationality { get; set; }
        public string Language { get; set; }
        public string JobTitle { get; set; }
        public string CompanyName { get; set; }
        public string AllowMail { get; set; }
        public string AllowEmail { get; set; }
        public string ExternalProfileID { get; set; }
        public string CendynPropertyId { get; set; }
        public string ExternalProfileID2 { get; set; }


        public string CMType { get; set; }
        public string CMCategory { get; set; }
        public bool IsPrimary { get; set; }

        public bool AllowMarketResearch { get; set; }
    }
}
