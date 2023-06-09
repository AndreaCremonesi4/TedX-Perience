const mongoose = require('mongoose');

const talk_schema = new mongoose.Schema({
_id: String,
title: String,
author: String,
date: String,
views: Number,
likes: Number,
url: String,
main_speaker: String,
details: String,
posted: String,
num_views: Number,
duration: String,
tags: Object,
watch_next_list: Object,
avg_views: Number
}, { collection: 'tedx_data' });

module.exports = mongoose.model('talk', talk_schema);
