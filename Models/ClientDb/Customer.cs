namespace CendynDataComparisonUtility.Models.ClientDb
{
    public class Customer
    {
        public int CustomerID { get; set; }
        public string SourceGuestId { get; set; }
        public string ShortTitle { get; set; }
        public string Salutation { get; set; }
        public string FirstName { get; set; } 
        public string LastName { get; set; } 
        public string Address1 { get; set; }
        public string City { get; set; }
        public string StateProvinceCode { get; set; }
        public string ZipCode { get; set; }
        public string CountryCode { get; set; }
        public string HomePhoneNumber { get; set; }
        public string FaxNumber { get; set; }
        public string Languages { get; set; }
        public string Nationality { get; set; }
        public string CellPhoneNumber { get; set; }
        public string BusinessPhoneNumber { get; set; }
        public string CompanyTitle { get; set; }
        public string JobTitle { get; set; }
        public string AllowEMail { get; set; }
        public string AllowMail { get; set; }
        public string AllowMarketResearch { get; set; }
        public string PhoneNumber { get; set; }
        public string Email { get; set; }
        public Guid PK_Profiles { get; set; }
    }
}
