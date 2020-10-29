import * as functions from 'firebase-functions';
import { DataSnapshot } from 'firebase-functions/lib/providers/database';

var admin = require('firebase-admin');
admin.initializeApp();

var db = admin.database();

//https://us-central1-prismatic-vial-174715.cloudfunctions.net/helloWorld
export const helloWorld = functions.https.onRequest(async (request, response) => {
    if (request.method !== 'POST') {
        response.sendStatus(400).send('You need to do a POST request!');
    }

    functions.logger.info('Nani');

    let words = request.body.words;

    let result: Map<string, [string, number][]> = new Map();

    let ranksMem: Map<string, number> = new Map();

    for (let word of words) {
        functions.logger.info(word);
        let urls = await getWordUrls(word);
        functions.logger.info('PROMISE ' + urls);

        if (!result.has(word)) {
            result.set(word, []);
        }

        for (let url of urls) {
            if (!ranksMem.has(url)) {
                let rank = await getUrlRank(url);
                if (rank === null) continue;

                ranksMem.set(url, parseFloat(rank));
            }

            functions.logger.info('RANK ' + ranksMem.get(url));
            let prevList = result.get(word)!;
            prevList.push([url, ranksMem.get(url)!]);
            result.set(word, prevList);
            //result.get(word)?.push([url, ranksMem.get(url)!]);
        }
    }

    functions.logger.info('HERE');
    functions.logger.info(result);
    let t = mapToObject(result);
    functions.logger.info(t);
    response.status(200).send(t);
});

function mapToObject(map: Map<string, [string, number][]>) {
    return Object.assign(Object.create(null), ...[...map].map(v => ({ [v[0]]: v[1] })));
}


async function getWordUrls(word: string) {
    let wordRef = db.ref('index/' + word);

    let urlGroup: DataSnapshot = await wordRef.once("value", function (x: DataSnapshot) {
        return ['asd'];
    });

    let keys: Array<string> = [];
    urlGroup.forEach(snap => {
        keys.push(snap.key);
        //functions.logger.info('PUSH ' + snap.key);
    });

    let urls: Array<string> = [];
    for (let key of keys) {
        let snap = urlGroup.child(key);
        //functions.logger.info('SNAP ' + snap.val());

        let val: string = snap.val();
        let partialUrls: Array<string> = val.split(' ');

        partialUrls.forEach(url => {
            urls.push(url);
        });
    }

    //functions.logger.info('URLS ' + urls);

    return urls;
}

async function getUrlRank(url: string) {
    functions.logger.info('SINGLE URL ' + url);
    if (url.replace(/\s/g, '').length === 0) {
        return null;
    }

    let urlRef = db.ref('ranks/' + url.replace(/\./g, '-'));
    let rankSnap = await urlRef.once('value', function (rank: DataSnapshot) {
        return [];
    });

    functions.logger.info(rankSnap.val());

    return rankSnap.val();
}

// // Start writing Firebase Functions
// // https://firebase.google.com/docs/functions/typescript
//
// export const helloWorld = functions.https.onRequest((request, response) => {
//   functions.logger.info("Hello logs!", {structuredData: true});
//   response.send("Hello from Firebase!");
// });
