const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
    _id: String,
    title: String,
    details: String,
    posted: String,
    url: String,
    num_views: Number,
    duration: String,
    likes: String,
    avg_points: Number,
    tags: Object,
    watch_next_list: Object
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);