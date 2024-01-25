const snowflake = require('snowflake-sdk')
const fs = require("fs");
const { connect } = require('http2');

function get_options() {
    if (fs.existsSync("/snowflake/session/token")) {
        return {
            accessUrl: "https://" + process.env.SNOWFLAKE_HOST,
            account: process.env.SNOWFLAKE_ACCOUNT,
            authenticator: 'OAUTH',
            token: fs.readFileSync('/snowflake/session/token', 'ascii'),
        }
    }
    else {
        return {
            account: process.env.SNOWFLAKE_ACCOUNT,
            username: process.env.SNOWFLAKE_USER,
            password: process.env.SNOWFLAKE_PASSWORD,
            warehouse: process.env.SNOWFLAKE_WAREHOUSE,
            database: process.env.SNOWFLAKE_DATABASE,
            schema: process.env.SNOWFLAKE_SCHEMA,
            clientSessionKeepAlive: true,
        }
    }
}

var connectionPool = null
function getPool(poolOpts = {}) {
    if (!poolOpts.min)
        poolOpts.min = connectionPool ? connectionPool.min : 1
    if (!poolOpts.max)
        poolOpts.max = connectionPool ? connectionPool.max : 10
    if (!poolOpts.testOnBorrow)
        poolOpts.testOnBorrow = connectionPool ? connectionPool.testOnBorrow :false

    if (connectionPool) {
        if ((connectionPool.min == poolOpts.min) && (connectionPool.max == poolOpts.max)) {
            return connectionPool
        }
    }

    try {
        connectionPool = snowflake.createPool(get_options(), poolOpts)
    }
    catch (error) {
        console.error("Error making connection pool: " + error.message)
    }

    return connectionPool
}

connectionPool = getPool()
module.exports = { connectionPool, getPool };
