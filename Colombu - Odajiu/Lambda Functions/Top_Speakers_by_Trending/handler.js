const connect_to_db = require('./db');
const talksModel = require('./Talk');

module.exports.Top_Speakers_by_Trending = (event, context, callback) => {
    context.callbackWaitsForEmptyEventLoop = false;
    console.log('Received event:', JSON.stringify(event, null, 2));
    let body = {}
    if (event.body) {
        body = JSON.parse(event.body)
    }

   if(body.num_video==null) {
       body.num_video = 25;
   }
   if(body.num_video < 1) {
       body.num_video = 1;
   }

    connect_to_db().then(async () => {
        
        try{
            let Top_Speaker_list_by_views = [];
            let Top_Speaker_list_by_likes = [];
            let talk_list_by_views = await talksModel.find({},{_id:0, title: 1, url:1, avg_views:1, main_speaker:1 }).sort({avg_views:-1}).limit(body.num_video);
            let talk_list_by_likes = await talksModel.find({},{_id:0, title: 1, url:1, likes:1, main_speaker:1 }).sort({likes:-1}).limit(body.num_video);
            
            talk_list_by_views.forEach((talk) =>{
                Top_Speaker_list_by_views.push({
                    main_speaker: talk.main_speaker
                });
            });
            
            talk_list_by_likes.forEach((talk) =>{
                Top_Speaker_list_by_likes.push({
                    main_speaker: talk.main_speaker
                });
            });

            let speakerCount = {};

            Top_Speaker_list_by_views.forEach((speaker) => {
                const mainSpeaker = speaker.main_speaker;
                speakerCount[mainSpeaker] = (speakerCount[mainSpeaker] || 0) + 1;
            });

            Top_Speaker_list_by_likes.forEach((speaker) => {
                const mainSpeaker = speaker.main_speaker;
                speakerCount[mainSpeaker] = (speakerCount[mainSpeaker] || 0) + 1;
            });

            let Top_Speakers_by_Trending_list = Object.keys(speakerCount).filter((speaker) => {
                return speakerCount[speaker] > 1;
            });

            callback(null, {
                statusCode: 200,
                body: JSON.stringify(Top_Speakers_by_Trending_list)
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
