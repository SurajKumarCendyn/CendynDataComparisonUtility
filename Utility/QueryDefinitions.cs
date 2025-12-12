namespace CendynDataComparisonUtility.Utility
{
    public static class QueryDefinitions
    {
        public static class EInDb
        {
            public const string Customer = @"SELECT DC.CustomerID
,DC.SourceGuestId
,DC.ShortTitle
,DC.FirstName
,DC.LastName
,DC.Salutation 
,DC.Address1 
,DC.City
,DC.StateProvinceCode
,DC.ZipCode
,DC.CountryCode
,DC.PhoneNumber
,DC.HomePhoneNumber
,DC.FaxNumber
,DC.Email
,DC.Lang_languageId as Languages
,DC.nationality
,DC.CellPhoneNumber
,DC.BusinessPhoneNumber
,DC.CompanyTitle
,DC.JobTitle
,DC.AllowEMail
,DC.AllowMail
,DC.PK_Profiles
,P.AllowMarketResearch
FROM [dbo].[D_Customer] DC WITH(NOLOCK) 
INNER JOIN [dbo].[PMS_Profiles] P WITH(NOLOCK) ON DC.PK_Profiles = P.PK_Profiles";
            public const string Stay = @"SELECT 
                                         Pk_Reservations
                                        ,SourceStayID
                                        ,CustomerID
                                        ,SourceGuestId
                                        ,ReservationNumber
                                        ,CendynPropertyID
                                        ,SubReservationNumber
                                        ,CentralReservation
                                        ,BookingEngConfNum
                                        ,StayStatus
                                        ,ArrivalDate
                                        ,DepartureDate
                                        ,ResCreationDate AS BookingDate 
                                        ,CancelDate
                                        ,GroupReservation
                                        ,Channel
                                        ,SourceOfBusiness
                                        ,MarketSeg
                                        ,MarketSubSeg
                                        ,RoomNights
                                        ,NumAdults
                                        ,NumChildren
                                        ,TotalPersons
                                        ,RateType
                                        ,RoomTypeCode
                                        ,RoomCode
                                        ,IATA
                                        ,NumRooms
                                        ,RoomRevenue
                                        ,Tax
                                        ,OtherRevenue
                                        ,TotalRevenue
                                        ,ExternalResID2
                                        FROM D_CUSTOMER_STAY R WITH (NOLOCK)";
            public const string StayDetail = @"SELECT  
                                             Pk_StayDetail
                                            ,SR.RateId
                                            ,SR.SourceStayId
                                            ,SR.CustomerId
                                            ,SR.ReservationNumber
                                            ,CS.CendynPropertyID
                                            ,SR.StayDate
                                            ,SR.StayRateType
                                            ,SR.StayRoomType
                                            ,SR.StayRateAmount
                                            ,SR.StayNumRooms
                                            ,SR.CurrencyCode 
                                            FROM D_CUSTOMER_STAY_RATE SR WITH(NOLOCK)
                                            INNER JOIN D_CUSTOMER_STAY CS ON CS.SourceStayId = SR.SourceStayId";
            public const string Transactions = @"SELECT 
                                                 T.PK_Transactions
                                                ,T.ExternalResID1
                                                ,T.ExternalProfileID
                                                ,T.CendynPropertyId
                                                ,T.TransactionSource
                                                ,T.TransactionGroup
                                                ,T.TransactionDate
                                                ,T.TransactionCode
                                                ,T.CurrencyCode
                                                ,T.CreditAmount
                                                ,T.DebitAmount
                                                FROM D_CUSTOMER_STAY_TRANSACTIONS T WITH (NOLOCK)";
        }
        public static class CenResDb
        {
            public const string Profiles = @"SELECT
                                             P.PK_Profiles
                                            ,P.Salutation
                                            ,P.FirstName 
                                            ,P.LastName  
                                            ,P.ExternalProfileID
                                            ,P.CendynPropertyId
                                            ,P.ExternalProfileID2  
                                            ,AD.Address1 
                                            ,AD.City
                                            ,AD.StateProvince
                                            ,AD.PostalCode
                                            ,AD.CountryCode 
                                            -- Phone columns based on CMCategory
                                            ,MAX(CASE WHEN CM.CMCategory IN ('Mobile','Cell') THEN CM.CMData END) AS PhoneNumber
                                            ,MAX(CASE WHEN CM.CMCategory = 'Home' THEN CM.CMData END) AS HomePhone
                                            ,MAX(CASE WHEN CM.CMCategory IN ('Business','Work') THEN CM.CMData END) AS WorkPhone
                                            ,MAX(CASE WHEN CM.CMCategory = 'Fax' THEN CM.CMData END) AS FaxNumber
                                            -- Email column
                                            ,MAX(CASE WHEN CME.CMCategory = 'Email' THEN CME.CMData END) AS Email
                                            ,P.Nationality
                                            ,P.PrimaryLanguage as language
                                            ,P.CompanyName
                                            ,P.AllowMail
                                            ,P.AllowEmail
                                            ,P.JobTitle
,P.AllowMarketResearch
                                            FROM [dbo].[Profiles] P WITH(NOLOCK) 
                                            LEFT JOIN [dbo].[ContactMethod] CM WITH(NOLOCK)
                                            ON CM.FK_Profiles = P.PK_Profiles
                                            AND CM.RecordStatus = 'Active'
                                            AND CM.CMType = 'Phone'
                                            AND CM.CMCategory IN ('Phone','Home','Mobile','Cell','Business','Work','Fax')
                                        LEFT JOIN [dbo].[ContactMethod] CME WITH(NOLOCK)
                                            ON CME.FK_Profiles = P.PK_Profiles
                                            AND CME.RecordStatus = 'Active'
                                            AND CME.CMType = 'IP'
                                            AND CME.CMCategory = 'Email'
                                            AND CME.IsPrimary = 1
                                        LEFT JOIN [dbo].[Address] AD WITH(NOLOCK)
                                            ON AD.FK_Profiles = P.PK_Profiles
                                            AND AD.RecordStatus = 'Active'";
            public const string Reservations = @"SELECT  
                                             PK_Reservations
                                            ,ExternalResID1
                                            ,ExternalResID2
                                            ,ExternalResId1 as ReservationNumber
                                            ,CendynPropertyID
                                            ,ConfirmationNum AS SubReservationNumber
                                            ,CentralResNum AS CentralReservation
                                            ,NULL AS BookingEngConfNum
                                            ,ResStatusCode AS StayStatus
                                            ,ResArriveDate AS  ArrivalDate
                                            ,ResDepartDate AS DepartureDate
                                            ,DateResMade AS  BookingDate
                                            ,CancellationDate AS CancelDate
                                            ,NULL AS GroupReservation
                                            ,NULL AS Channel
                                            ,NULL AS SourceOfBusiness
                                            ,MarketSegmentCode AS MarketSeg
                                            ,NULL AS MarketSubSeg
                                            ,TotalRoomNights AS  RoomNights
                                            ,NumAdults
                                            ,NumChildren
                                            ,ISNULL(NumAdults,0) + ISNULL(NumYouths,0) + ISNULL(NumChildren,0) AS TotalPersons
                                            ,NULL RateType
                                            ,RoomTypeCode
                                            ,NULL AS RoomCode
                                            ,NULL AS IATA
                                            ,NumRooms
                                            ,TotalRoomRevenue AS   RoomRevenue
                                            ,TotalTax AS  Tax
                                            ,TotalOtherRevenue AS OtherRevenue
                                            ,TotalRevenue
                                             FROM [Reservations] R WITH (NOLOCK)";
            public const string StayDetail = @"SELECT 
                                             SD.PK_StayDetail
                                            ,R.ExternalResID1 AS ReservationNumber
                                            ,R.CendynPropertyID
                                            ,SD.StayDate
                                            ,SD.SourceRateType AS StayRateType
                                            ,SD.SourceRoomType AS StayRoomType
                                            ,SD.DailyRate AS StayRateAmount 
                                            ,SD.NumberOfRooms AS StayNumRooms
                                            ,SD.CurrencyCode
                                            FROM StayDetail SD WITH (NOLOCK)
                                            INNER JOIN StayDetailHeader SH WITH(NOLOCK) 
                                            ON SH.PK_StayDetailHeader = SD.FK_StayDetailHeader
                                            INNER JOIN Reservations R WITH(NOLOCK) 
                                            ON R.PK_Reservations = SH.FK_Reservations";
            public const string Transactions = @"SELECT 
                                                 T.PK_Transactions
                                                 ,T.TransactionID
                                                ,T.ExternalResID1
                                                ,T.ExternalProfileID
                                                ,T.CendynPropertyId
                                                ,T.TransactionSource
                                                ,T.TransactionGroup
                                                ,T.TransactionDate
                                                ,T.TransactionCode
                                                ,T.CurrencyCode
                                                ,T.CreditAmount
                                                ,T.DebitAmount
                                                FROM Transactions T WITH (NOLOCK)";
        }
        public static class CenResNormalizeDb
        {
            public const string Customer = @"SELECT 
         C.CustomerID
        ,C.SourceGuestID
        ,C.ShortTitle
        ,C.FirstName
        ,C.LastName
        ,C.Salutation
        ,C.Address1
        ,C.City
        ,C.StateProvinceCode
        ,C.ZipCode
        ,C.CountryCode
        ,C.PhoneNumber
        ,C.HomePhoneNumber
        ,C.FaxNumber
        ,C.Email
        ,C.Languages
        ,C.Nationality
        ,C.CellPhoneNumber
        ,C.BusinessPhoneNumber
        ,C.CompanyTitle
        ,C.JobTitle
        ,C.AllowEMail
        ,C.AllowMail
        FROM CCRM.CUSTOMER C WITH(NOLOCK)";
            public const string Stays = @"SELECT ReservationNumber
                ,SubReservationNumber
                ,CentralReservation
                ,ResStatusCode as StayStatus
                ,ArrivalDate
                ,DepartureDate
                ,BookingDate
                ,CancelDate
                ,GroupReservation
                ,Channel
                ,SourceOfBusiness
                ,MarketSeg
                ,MarketSubSeg
                ,RoomNights
                ,NumAdults
                ,NumChildren
                ,TotalPersons
                ,RateType
                ,RoomTypeCode
                ,RoomCode
                ,IATA
                ,NumRooms
                ,RoomRevenue
                ,Tax
                ,OtherRevenue
                ,TotalRevenue FROM CCRM.Stays WITH(NOLOCK)";
            public const string StayDetail = @"SELECT  
                                             S.ReservationNumber
                                            ,SD.StayDate
                                            ,SD.StayRateType
                                            ,SD.StayRoomType
                                            ,SD.StayRateAmount
                                            ,SD.StayNumRooms
                                            ,SD.CurrencyCode
                                            FROM  CCRM.StayDetail SD WITH(NOLOCK) 
                                            INNER JOIN CCRM.Stays S on S.StayId= SD.StayId";
            public const string Transactions = @"SELECT StayTransactionsID,TransactionSource
                                                        ,TransactionGroup
                                                        ,TransactionDate
                                                        ,TransactionCode
                                                        ,CurrencyCode
                                                        ,CreditAmount
                                                        ,DebitAmount FROM CCRM.StayTransactions WITH(NOLOCK)";
        }
        // For MongoDB, you can use filter builders or store collection names
        public static class MongoDb
        {
            public const string ContactsCollection = "contacts";
            public const string PurchasesCollection = "purchases";
            public const string TransactionsCollection = "transactions";
        }
    }
}
