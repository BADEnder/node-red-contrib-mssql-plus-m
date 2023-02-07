module.exports = function (RED) {
    'use strict';

    const sql = require('mssql');

    function connection(config) {
        RED.nodes.createNode(this, config);
        var node = this;

        node.config = {
            user: node.credentials.username,
            password: node.credentials.password,
            domain: node.credentials.domain,
            server: config.server,
            database: config.database,
            options: {
                port: config.port ? parseInt(config.port) : undefined,
                tdsVersion: config.tdsVersion,
                encrypt: config.encyption,
                useUTC: config.useUTC,
                connectTimeout: config.connectTimeout ? parseInt(config.connectTimeout) : undefined,
                requestTimeout: config.requestTimeout ? parseInt(config.requestTimeout) : undefined,
                cancelTimeout: config.cancelTimeout ? parseInt(config.cancelTimeout) : undefined,
                camelCaseColumns: config.camelCaseColumns == "true" ? true : undefined,
                parseJSON: config.parseJSON,

            },
            pool: {
                max: parseInt(config.pool),
                min: 0,
                idleTimeoutMillis: 3000,
                log: (message, logLevel) => console.log(`POOL: [${logLevel}] ${message}`)
            }
        };

        //config options seem to differ between pool and tedious connection
        //so for compatibility I just repeat the ones that differ so they get picked up in _poolCreate ()
        node.config.port = node.config.options.port;
        node.config.connectionTimeout = node.config.options.connectTimeout;
        node.config.requestTimeout = node.config.options.requestTimeout;
        node.config.cancelTimeout = node.config.options.cancelTimeout;
        node.config.encrypt = node.config.options.encrypt;
        
        node.connectionPool = new sql.ConnectionPool(node.config)

    }

    RED.nodes.registerType('MSSQL-CN', connection, {
        credentials: {
            username: {
                type: 'text'
            },
            password: {
                type: 'password'
            },
            domain: {
                type: 'text'
            }
        }
    });

    function mssql(config) {
        RED.nodes.createNode(this, config);
        var mssqlCN = RED.nodes.getNode(config.mssqlCN);
        var node = this;

        node.query = config.query;
        node.outField = config.outField;
        node.returnType = config.returnType;
        node.throwErrors = !config.throwErrors || config.throwErrors == "0" ? false : true;

        var setResult = function (msg, field, data, returnType = 0 ) {

            if (returnType == 1) {
                msg[field] = data
            } else {
                msg[field] = data.recordset
            }
            return msg
        };

        node.processError = function(err,msg){
            let errMsg = "Error";
            if(typeof err == "string"){
                errMsg = err;
                msg.error = err;
            } else if(err && err.message) {
                errMsg = err.message;
                //Make an error object from the err.  NOTE: We cant just assign err to msg.error as a promise 
                //rejection occurs when the node has 2 wires on the output. 
                //(redUtil.cloneMessage(m) causes error "node-red Cannot assign to read only property 'originalError'")
                msg.error = {
                    class: err.class,
                    code: err.code, 
                    lineNumber: err.lineNumber, 
                    message: err.message, 
                    name: err.name, 
                    number: err.number, 
                    procName: err.procName, 
                    serverName: err.serverName, 
                    state: err.state, 
                    toString: function(){
                        return this.message;
                    }
                }
            }
            
            node.status({
                fill: 'red',
                shape: 'ring',
                text: errMsg
            });

            if(node.throwErrors){
                node.error(msg.error,msg);
            } else {
                node.log(err);
                node.send(msg);
            }
        }    

        node.on('input', function (msg) {
            
            node.status({}); //clear node status
            delete msg.error; //remove any .error property passed in from previous node
            msg.query = node.query || msg.payload;
            try {
                doSQL(node, msg);
            } catch (err) {
                node.processError(err,msg)
            }
            
            
 
        });
   
        async function doSQL(node, msg){
            node.status({
                fill: 'blue',
                shape: 'dot',
                text: 'requesting'
            });

            try {
                const pool = await mssqlCN.connectionPool.connect()
                const data = await pool.query(msg.query)
                
                msg = setResult(msg, node.outField, data, node.returnType)
                node.send(msg)
                node.status({
                    fill: 'green',
                    shape: 'dot',
                    text: 'done'
                })
            } catch (err) {
                node.processError(err,msg)
            }


        }

        node.on('close', async function () {
            await sql.close()
            node.log(`Disconnecting server : ${mssqlCN.config.server}, database : ${mssqlCN.config.database}, port : ${mssqlCN.config.options.port}, user : ${mssqlCN.config.user}`);
        });
    }
    RED.nodes.registerType('MSSQL', mssql);
};

