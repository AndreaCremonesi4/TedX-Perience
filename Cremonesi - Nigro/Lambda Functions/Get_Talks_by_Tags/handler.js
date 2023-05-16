const connect_to_db = require('./db');

// GET BY TALK HANDLER

const talksDB = require('./Talk');

module.exports.get_top_tags = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }

    if (body.talks_depth == null) {
        body.talks_depth = 10
    }
    if (body.n_tags == null) {
        body.n_tags = 10
    }
    
    if(body.talks_depth < 1){
        body.talks_depth = 1
    }
    if(body.n_tags < 1){
        body.n_tags = 1
    }

    connect_to_db().then(async () => {
        try{
            let tags_list = [];
            let talks_list = await talksDB.find({},{tags:1,_id:0}).sort({avg_points:-1}).limit(body.talks_depth);
            
            talks_list.forEach((talk) =>{
               let tags = talk.tags;
               
               tags.forEach((tag) =>{
                    tags_list.push(tag); 
               });
            })
            
            const counts = {};
            for (const num of tags_list){
                counts[num] = counts[num] ? counts[num] + 1 : 1;
            }
            
            const map = new Map(Object.entries(counts));
            const mapSort = new Map([...map.entries()].sort((a, b) => b[1] - a[1]));
            
            let top_tags = Array.from(mapSort.keys());
            top_tags = top_tags.slice(0, body.n_tags)
            
            callback(null, {
                statusCode: 200,
                body: JSON.stringify(top_tags)
                
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