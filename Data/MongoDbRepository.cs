using CendynDataComparisonUtility.Models.MongoDb;
using CendynDataComparisonUtility.Utility; 
using MongoDB.Bson;
using MongoDB.Driver;

namespace CendynDataComparisonUtility.Data
{
    public class MongoDbRepository
    {
        private readonly string _connectionString;
        private readonly string _mongoDbName;
        public MongoDbRepository(string connectionString, string mongoDbName, string accountId) =>
            (_connectionString, _mongoDbName) = (connectionString, mongoDbName);

        //Get Customers
        public List<Contacts> GetContacts(List<MongoCenResMap> mapping, string accountId)
        {
            var mongoClient = new MongoClient(_connectionString);
            var db = mongoClient.GetDatabase(_mongoDbName);
            var contactsCollection = db.GetCollection<Contacts>(QueryDefinitions.MongoDb.ContactsCollection);
            var filter = Builders<Contacts>.Filter.In(c => c.UserId, mapping.Select(x=>x.UserIds)) &
                         Builders<Contacts>.Filter.Eq(c => c.AccountId, new ObjectId(accountId)); 

            var cursor = contactsCollection.Find(filter);
            var contacts = cursor.ToList();
            // Set Pk_Profiles for each contact based on mapping
            foreach (var contact in contacts)
            {
                var map = mapping.FirstOrDefault(m => m.UserIds == contact.UserId);
                if (map != null)
                {
                    contact.Pk_Profiles = map.Pk_Profiles;
                }
            }
            return contacts;
        }

        //Get Reservations
        public List<Purchases> Purchases(List<MongoResMap> mongoRes , string accountId)
        {
            var mongoClient = new MongoClient(_connectionString);
            var db = mongoClient.GetDatabase(_mongoDbName);
            var purchasesCollection = db.GetCollection<Purchases>(QueryDefinitions.MongoDb.PurchasesCollection);

            var filter = Builders<Purchases>.Filter.In(c => c.Uuid_CendynPropertyID, mongoRes.Select(x => x.CendynPropertyId)) &
                         Builders<Purchases>.Filter.In(c => c.UniqId_ExternalResID1, mongoRes.Select(x => x.ReservationNo)) &
                         Builders<Purchases>.Filter.Eq(c => c.AccountId, new ObjectId(accountId));
            var cursor = purchasesCollection.Find(filter);
            var reservations = cursor.ToList();
            return reservations;
        }

        //GetStayDetails
        public List<StayDetail> StayDetails(List<MongoResMap> mongoRes, string accountId)
        {
            var mongoClient = new MongoClient(_connectionString);
            var db = mongoClient.GetDatabase(_mongoDbName);
            var collection = db.GetCollection<Purchases>(QueryDefinitions.MongoDb.PurchasesCollection);

            var filter = Builders<Purchases>.Filter.In(c => c.Uuid_CendynPropertyID, mongoRes.Select(x => x.CendynPropertyId)) &
                         Builders<Purchases>.Filter.In(c => c.UniqId_ExternalResID1, mongoRes.Select(x => x.ReservationNo)) &
                         Builders<Purchases>.Filter.Eq(c => c.AccountId, new ObjectId(accountId));
            var projection = Builders<Purchases>.Projection.Include(p => p.PurchaseStayDetails)
                                                           .Include(p => p.UniqId_ExternalResID1)
                                                           .Include(p => p.Uuid_CendynPropertyID);

            var purchases = collection.Find(filter).Project<Purchases>(projection).ToList();

            var stayDetails = new List<StayDetail>();
            foreach (var purchase in purchases)
            {
                if (purchase.PurchaseStayDetails != null)
                {
                    foreach (var stayDetail in purchase.PurchaseStayDetails)
                    {
                        stayDetail.UniqId_ExternalResID1 = purchase.UniqId_ExternalResID1;
                        stayDetail.Uuid_CendynPropertyID = purchase.Uuid_CendynPropertyID;
                        stayDetails.Add(stayDetail);
                    }
                }
            }
            return stayDetails;
        }

        //Get Transactions
        public List<Transactions> Transactions(List<MongoTransactionMap> mongoTrans, string accountId)
        {
            var mongoClient = new MongoClient(_connectionString);
            var db = mongoClient.GetDatabase(_mongoDbName);
            var transactionsCollection = db.GetCollection<Transactions>(QueryDefinitions.MongoDb.TransactionsCollection);
            var filter = Builders<Transactions>.Filter.In(c => c.ExternalResId1, mongoTrans.Select(x => x.ExternalResId1)) &
                         Builders<Transactions>.Filter.In(c => c.TransactionId, mongoTrans.Select(x => x.TransactionId)) &
                         Builders<Transactions>.Filter.Eq(c => c.AccountId, new ObjectId(accountId));
            var cursor = transactionsCollection.Find(filter);
            var transactions = cursor.ToList();
            return transactions;
        }


        public class MongoCenResMap()
        {
            public Guid Pk_Profiles { get; set; }
            public string UserIds { get; set; }
        }

        public class MongoResMap()
        {
            public string ReservationNo { get; set; }
            public string CendynPropertyId { get; set; }
        }

        public class MongoTransactionMap() {
            public Guid Pk_Transactions { get; set; }
            public string ExternalResId1 { get; set; }
            public string TransactionId { get; set; }

        }
    }
}
