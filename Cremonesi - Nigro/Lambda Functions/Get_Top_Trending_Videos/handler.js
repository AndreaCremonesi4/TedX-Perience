const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talksModel = require('./Talk');

module.exports.get_top_trending_videos = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
   
    if(body.num_video == null) {
        body.num_video = 10;
    }
    if(body.num_video < 1) {
        body.num_video = 1;
    }
   
    connect_to_db().then(async () => {
        try{
            let top_trending_list = await talksModel.find({},{_id:1, title: 1, url:1, avg_points:1}).sort({avg_points:-1}).limit(body.num_video); 

            callback(null, {
                statusCode: 200,
                body: JSON.stringify(top_trending_list)
            })
        }
        catch(err){
            callback(null, {
                statusCode: err.statusCode || 500,
                headers: { 'Content-Type': 'text/plain' },
                body: 'Could not fetch the next talk data.'
            });
        }
    });
};