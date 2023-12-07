const express = require('express');
const {API_PORT = 8081} = process.env
const snowflake = require('./spcs_helpers/connection.js')

// create express app, set up json parsing and logging
const app = express();
app.use(express.json());
app.use((req,res,next) => {
    console.log(req.method + ": " + req.path)
    next()
})

// Start the server
app.listen(API_PORT, () => {
    console.log("Server running on port " + API_PORT);
});

// Create Snowflake connection pool with default min/max
connectionPool = snowflake.getPool()

// api routes
app.get("/top_clerks", (req, res, next) => {
    var start_range = '1995-01-01'
    var end_range = '1995-03-31'
    var topn = 10
    try {
        if (req.query.start_range)
            start_range = new Date(req.query.start_range).toISOString().split('T')[0]
        if (req.query.end_range)
            end_range = new Date(req.query.end_range).toISOString().split('T')[0]
        if (req.query.topn)
            topn = Number(req.query.topn)
            if (!Number.isInteger(topn))
                throw new Error("Supplied topn is not an integer")
    }
    catch (error) {
        console.log("ERROR: Invalid parameters. " + error.message)
        next(new Error("Invalid parameters"))
        return
    }

    const sql = `
        SELECT
            o_clerk
            , SUM(o_totalprice) AS clerk_total
        FROM snowflake_sample_data.tpch_sf10.orders
        WHERE o_orderdate >= :1
            AND o_orderdate <= :2
        GROUP BY o_clerk
        ORDER BY clerk_total DESC
        LIMIT ${topn}
    `
    const bindVars = [start_range, end_range]
    connectionPool.use(async (connection) => {
        const statement = await connection.execute({
            sqlText: sql,
            binds: bindVars,
            complete: function(err, stmt, rows) {
                if (err) {
                    console.error("Failed to execute query: " + err.message)
                    next(err)
                }
                else {
                    console.log("Succesfully queried, nrows=" + rows.length)
                    res.send(rows)
                }
            }
        })
    })
})
