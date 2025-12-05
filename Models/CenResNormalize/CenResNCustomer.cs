namespace CendynDataComparisonUtility.Models.CenResNormalize
{
    public class CenResNCustomer
    {
        public string CustomerId { get; set; }
        public string SourceGuestID { get; set; }
        public string ShortTitle { get; set; }
        public string FirstName { get; set; }
        public string LastName { get; set; }
        public string Salutation { get; set; }
        public string Address1 { get; set; }
        public string City { get; set; }
        public string StateProvince { get; set; }
        public string ZipCode { get; set; }
        public string CountryCode { get; set; }
        public string PhoneNumber { get; set; }
        public string HomePhoneNumber { get; set; }
        public string FaxNumber { get; set; }
        public string Email { get; set; }
        public string Languages { get; set; }
        public string Nationality { get; set; }
        public string CellPhoneNumber { get; set; }
        public string BusinessPhoneNumber { get; set; }
        public string CompanyTitle { get; set; }
        public string JobTitle { get; set; }
        public string AllowEMail { get; set; }
        public string AllowMail { get; set; }
        public DateTime DateInserted { get; set; }
        public DateTime LastUpdated { get; set; }
        public Guid Pk_Profiles { get; internal set; }
    }
}
