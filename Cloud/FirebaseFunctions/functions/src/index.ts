import * as functions from 'firebase-functions';

//var admin = require('firebase-admin');
//var db = admin.database();

//https://us-central1-prismatic-vial-174715.cloudfunctions.net/helloWorld
export const helloWorld = functions.https.onRequest((request, response) => {
    if (request.method !== 'POST') {
        response.sendStatus(400).send('You need to do a POST request!');
    }

    //var wordsRef = db.ref('index');
    let data = request.body;

    functions.logger.info(JSON.stringify(data));

    response.sendStatus(200);
});
// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript
//
// export const helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });
