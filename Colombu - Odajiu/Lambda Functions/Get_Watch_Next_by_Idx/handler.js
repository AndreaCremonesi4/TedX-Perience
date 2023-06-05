const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talksModel = require('./Talk');

module.exports.get_watch_next_by_idx = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
    // set default
    if (!body.id) {
        callback(null, {
            statusCode: 500,
            headers: {
                'Content-Type': 'text/plain'
            },
            body: 'Could not fetch the talks. Id is null.'
        })
    }

    connect_to_db().then(async () => {
        let next_talks_data_list = [];

        try{
            let talk = await talksModel.findOne({_id: body.id});
            let watch_next_ids = talk.watch_next_list;
            
            for(let i = 0; i < watch_next_ids.length; i++){
                let next_talk = await talksModel.findOne({_id: watch_next_ids[i]}, {title:1, url:1});

                next_talks_data_list.push(next_talk);
            }
            
            callback(null, {
                statusCode: 200,
                body: JSON.stringify(next_talks_data_list)
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