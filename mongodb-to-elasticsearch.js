// Simple NodeJS app for detecting and applying changes in collection in MongoDB with ElasticSearch in real time
// Aplicación simple de NodeJS para detectar y modificar una colección de MongoDB con ElasticSearch en tiempo real

const MongoClient = require("mongodb").MongoClient
const { Client } = require('@elastic/elasticsearch')

const esclient = new Client({
  node: 'YOUR_ELASTICSEARCH_ADDRESS',
  auth: {
    username: 'YOUR_ELASTICSEARCH_USERNAME',
    password: 'YOUR_ELASTICSEARCH_PASSWORD'
  }
})

async function init() {
  const mongoclient = await MongoClient.connect(config.mongoUrl, { useUnifiedTopology: true })
  .catch(err => { console.log(err); });

  if (!mongoclient)
    return console.log('No client');

  try {

    const db = mongoclient.db('cheflist');
    //const collection = db.collection('products');
    await db.command({ ping:1 })
    console.log("MongoDB ElasticSearch sync: Connected MongoDB server")

    //Wait for changes with operations insert, update or replace and update ES database
    async function detectInsertUpdateReplaceChangeStream() {
      const changeStream = (await db.collection('products')).watch([
        {
          "$match": {
            "operationType": {
              "$in": ["insert", "update", "replace"]
            }
          }
        },
        {
          "$project": {
            "documentKey": false
          }
        }
      ], {"fullDocument": "updateLookup"});
      return changeStream;
    }

    async function detectDeleteChangeStream() {
      const changeStream = (await db.collection('products')).watch([
        {
          "$match": {
            "operationType": {
              "$in": ["delete"]
            }
          }
        },
        {
          "$project": {
            "documentKey": true
          }
        }
      ]);
      return changeStream;
    }

    const insertUpdateReplaceChangeStream = await detectInsertUpdateReplaceChangeStream();
    upsertChangeStream.on("change", async next => {
      console.log("Pushing document to Elasticsearch, id:", next.fullDocument._id);
      next.fullDocument.id = next.fullDocument._id;
      Reflect.deleteProperty(next.fullDocument, "_id");
      const response = await esclient.index({
        "id": next.fullDocument.id,
        "index": "YOUR_DESIRED_ELASTICSEARCH_INDEX",
        "body": next.fullDocument,
        "type": "doc"
      });
      console.log("Document upserted, status code:", response.statusCode);
    });
    insertUpdateReplaceChangeStream.on("error", error => {
      console.error(error);
    });


    const deleteChangeStream = await detectDeleteChangeStream();
    deleteChangeStream.on("change", async next => {
      console.log("Deleting data from ElasticSearch, id", next.documentKey._id);
      const response = await esclient.delete({
        "id": next.documentKey._id,
        "index": "YOUR_DESIRED_ELASTICSEARCH_INDEX",
        "type": "doc"
      });
      console.log("Document successsfully deleted, status code: ", response.statusCode);
    });
    deleteChangeStream.on("error", error => {
      console.error(error);
    });

  } catch(err) {
    console.log(err);
  }

}

init();
