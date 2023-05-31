const connect_to_db = require('./db');
const talksModel = require('./Talk');

module.exports.Most_Liked = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }
   
   if(body.num_video==null) {
       body.num_video = 10;
   }
   if(body.num_video < 1) {
       body.num_video = 1;
   }
   
    connect_to_db().then(async () => {

        try{
            let most_liked_list = [];
            let talk_list = await talksModel.find({},{_id:0, title: 1,url:1, likes:1}).sort({likes:-1}).limit(body.num_video); 
            
             talk_list.forEach((talk) =>{
                most_liked_list.push({
                    Title: talk.title, 
                    URL: talk.url,
                    Likes: talk.likes
                    
                });            
                 
             });
         
            callback(null, {
                statusCode: 200,
                body: JSON.stringify(most_liked_list)
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
