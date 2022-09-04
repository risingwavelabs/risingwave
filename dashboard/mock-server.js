const express = require('express');

const app = express();
app.listen(3000, () => {
    console.log("Server running on port 3000");
})

app.get("/actors", (req, res, next) => {
    res.json(require('./mock/actors.json'));
})

app.get("/fragments", (req, res, next) => {
    res.json(require('./mock/fragments.json'));
})

app.get("/materialized_views", (req, res, next) => {
    res.json(require('./mock/materialized_views.json'));
})


app.get("/clusters/0", (req, res, next) => {
    res.json(require('./mock/cluster_0.json'));
})

app.get("/clusters/1", (req, res, next) => {
    res.json(require('./mock/cluster_1.json'));
})

app.get("/clusters/2", (req, res, next) => {
    res.json(require('./mock/cluster_2.json'));
})
