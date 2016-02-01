/**
 * Copyright 2015 mcarboni@redant.com
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/



 module.exports = function(RED) {
    "use strict";
    var knex=require('knex');

    RED.httpAdmin.get('/sqldb/:id',function(req,res) {
        var credentials = RED.nodes.getCredentials(req.params.id);
        if (credentials) {
            res.send(JSON.stringify({user:credentials.user,hasPassword:(credentials.password&&credentials.password!="")}));
        } else {
            res.send(JSON.stringify({}));
        }
    });

    RED.httpAdmin.delete('/sqldb/:id',function(req,res) {
        RED.nodes.deleteCredentials(req.params.id);
        res.send(200);
    });

    RED.httpAdmin.post('/sqldb/:id',function(req,res) {
        var body = "";
        req.on('data', function(chunk) {
            body+=chunk;
        });
        req.on('end', function(){
            var newCreds = querystring.parse(body);
            var credentials = RED.nodes.getCredentials(req.params.id)||{};
            if (newCreds.user == null || newCreds.user == "") {
                delete credentials.user;
            } else {
                credentials.user = newCreds.user;
            }
            if (newCreds.password == "") {
                delete credentials.password;
            } else {
                credentials.password = newCreds.password||credentials.password;
            }
            RED.nodes.addCredentials(req.params.id,credentials);
            res.send(200);
        });
    });

    function InjectPayload(msg,payload) {
        var result = {};
        for (var i in msg) {
            result[i] = msg[i];
        }
        result.payload = payload;
        return result;
    }

    function _getConnection(config) {
        console.log('Creating a',config.dialect,'connection');
        switch (config.dialect) {
            case 'pg':
                return knex({
                    client: 'pg',
                    connection: {
                        user        :  config.user,
                        password    :  config.password,
                        host        :  config.hostname,
                        port        :  config.port,
                        database    :  config.db,
                        ssl         :  config.ssl
                    },
                    pool: {
                        min: config.minPool,
                        max: config.maxPool
                    },
                    debug: true
                });
            case 'mysql':
                return knex({
                    client: 'mysql',
                    connection: {
                        user        :  config.user,
                        password    :  config.password,
                        host        :  config.hostname,
                        database    :  config.db
                    },
                    pool: {
                        min: config.minPool,
                        max: config.maxPool
                    },
                    debug: true
                });
            case 'sqlite3':
                return knex({
                    client: 'sqlite3',
                    connection: {
                        filename    :  config.filename
                    },
                    debug: true
                });
        }

    }

    function SqlDatabaseNode(n) {
        RED.nodes.createNode(this,n);
        this.dialect = n.dialect;
        this.filename = n.filename;
        this.hostname = n.hostname;
        this.port = n.port;
        this.db = n.db;
        this.ssl = n.ssl;
        this.minPool = n.minpool || 0;
        this.maxPool = n.maxpool || 2;

    	var credentials = this.credentials;
    	if (credentials) {
    		this.user = credentials.user;
    		this.password = credentials.password;
    	}

        this.connection = _getConnection(this);
    }

    function SqlArrayNode(n) {
        RED.nodes.createNode(this,n);

        try {
            this.columns = JSON.parse(n.columns);
        } catch (e) {
            node.error(e.message);
            this.columns = [];
        }
    }


    RED.nodes.registerType("sqldb",SqlDatabaseNode,{
            credentials: {
                user: {type:"text"},
                password: {type: "password"}
            }
        });
    RED.nodes.registerType("sqlarray",SqlArrayNode);



    function _prepareColumns(payload,columns) {
        var allColumns = columns.length === 0;
        if ((typeof payload !== "object") || (payload === null) || (Array.isArray(payload))) {
            throw new Error("Invalid payload type "+(typeof payload));
        }
        var cols = {},
            ok = true;
        for (var key in payload) {
            if (payload.propertyIsEnumerable(key)) {
                if ( allColumns || ( columns.indexOf(key.toLowerCase()) !== -1)) {
                    if (typeof payload[key] !== "object") {
                        cols[key.toLowerCase()] = payload[key];
                    } else {
                        throw new Error("Invalid property type "+typeof payload[key]+"\n"+JSON.stringify(payload[key],null,4));
                        ok = false;
                        break;
                    }
                }
            }
        }
        if (ok) {
            return cols;
        }
    }

    function SqlNodeInsert(n) {
        var node = this;

        RED.nodes.createNode(this,n);

        this.allColumns = n.columns.length === 0;
        this.sqlConfig = RED.nodes.getNode(n.sqldb);
        this.columns = RED.nodes.getNode(n.columns).columns;
        this.requireAll = n.requireAll;
        this.table = n.table;

        this.on("input",function (msg) {
            try {
                var cols = _prepareColumns(msg.payload,node.columns);
                if ( !node.requireAll || (node.columns.length === Object.keys(cols).length ) ) {
                    //Build query
                    node.sqlConfig.connection
                        (node.table).insert(cols).then(function (result) {
                            node.send(InjectPayload(msg,result));
                        }).catch(function (e) {
                            node.error(e);
                        });
                } else {
                    node.error("One or more columns are missing");
                }
            } catch (e) {
                node.error(e.message);
            }
        });
    }


    function SqlNodeUpdate(n) {
        var node = this;

        RED.nodes.createNode(this,n);

        this.allColumns = n.columns.length === 0;
        this.sqlConfig = RED.nodes.getNode(n.sqldb);
        this.columns = RED.nodes.getNode(n.columns).columns;
        this.where = RED.nodes.getNode(n.where).columns;
        this.requireAll = n.requireAll;
        this.requireAllWhere = n.requireAllWhere;
        this.table = n.table;

        this.on("input",function (msg) {
            try {
                var cols = _prepareColumns(msg.payload,node.columns),
                    where= {};
                if (msg.where) {
                    where = _prepareColumns(msg.where,node.where);
                }
                if ( !node.requireAll || (node.columns.length === Object.keys(cols).length ) ) {
                    //Build query
                    var query = node.sqlConfig.connection(node.table).update(cols);
                    if ( !node.requireAllWhere || (node.where.length === Object.keys(where).length ) ) {
                        if (Object.keys(where).length) {
                            query = query.where(where);
                        }
                        query.then(function (rows) {
                            node.send(InjectPayload(msg,rows));
                        }).catch(function (e) {
                            node.error(e);
                        });
                    }
                } else {
                    node.error("One or more columns are missing");
                }
            } catch (e) {
                node.error(e.message);
            }
        });
    }

    function SqlNodeSelect(n) {
        var node = this;

        try {
            RED.nodes.createNode(this,n);

            this.allColumns = n.columns.length === 0;
            this.sqlConfig = RED.nodes.getNode(n.sqldb);
            this.columns = RED.nodes.getNode(n.columns).columns;
            this.where = RED.nodes.getNode(n.where).columns;
            this.group = RED.nodes.getNode(n.group).columns;
            this.order = RED.nodes.getNode(n.order).columns;
            this.orderdir = n.orderdir;
            this.table = n.table;
            this.noWhere = n.noWhere;
            this.limit = n.limit || 0;
            this.offset = n.offset || 0;

            node.on("input",function (msg) {
                try {
                    var where = {};
                    if (!node.noWhere) {
                        where = _prepareColumns(msg.payload,node.where);
                    }
                    if ( !node.requireAll || (node.columns.length === Object.keys(where).length ) ) {
                        //Build query
                        var query = node.sqlConfig.connection(node.table),
                        limit = parseInt(node.limit),
                        offset = parseInt(node.offset),
                        group = node.group,
                        order = node.order;

                        if (!limit || isNaN(limit))  {
                            limit = msg.limit ? parseInt(msg.limit) : 0;
                        }

                        if (!offset || isNaN(offset))  {
                            offset = msg.offset ? parseInt(msg.offset) : 0;
                        }

                        query = (node.allColumns ? query.select() : query.select(node.columns));

                        if (limit && !isNaN(limit)) {
                            query = query.limit(limit);
                        }

                        if (offset && !isNaN(offset)) {
                            query = query.offset(offset);
                        }

                        if (group.length) {
                            query = query.groupBy(group);
                        }

                        if (order.length) {
                            query = query.orderBy(order,node.orderdir);
                        }

                        if (Object.keys(where).length > 0) {
                            query = query.where(where);
                        }

                        query.then(function (rows) {
                            node.send(InjectPayload(msg,rows));
                        }).catch(function (e) {
                            node.error(e);
                        });
                    } else {
                        node.error("One or more columns are missing");
                    }
                } catch (e) {
                    node.error(e.message);
                }
            });
        } catch(e) {
            console.error(e);
        }

    }

    function SqlNodeDelete(n) {
        var node = this;

        try {
            RED.nodes.createNode(this,n);

            this.allColumns = n.columns.length === 0;
            this.sqlConfig = RED.nodes.getNode(n.sqldb);
            this.where = RED.nodes.getNode(n.where).columns;
            this.table = n.table;
            this.limit = n.limit || 0;

            node.on("input",function (msg) {
                try {
                    var where = _prepareColumns(msg.payload,node.where);
                    if ( !node.requireAll ) {
                        //Build query
                        var query = node.sqlConfig.connection(node.table),
                            limit = parseInt(node.limit);

                        if (!limit || isNaN(limit))  {
                            limit = msg.limit ? parseInt(msg.limit) : 0;
                        }

                        if (limit && !isNaN(limit)) {
                            query = query.limit(limit);
                        }

                        if (Object.keys(where).length > 0) {
                            query = query.where(where);
                        }

                        query.del().then(function (rows) {
                            node.send(InjectPayload(msg,rows));
                        }).catch(function (e) {
                            node.error(e);
                        });
                    } else {
                        node.error("One or more columns are missing");
                    }
                } catch (e) {
                    node.error(e.message);
                }
            });
        } catch(e) {
            console.error(e);
        }

    }

    RED.nodes.registerType("SQL Insert",SqlNodeInsert);
    RED.nodes.registerType("SQL Update",SqlNodeUpdate);
    RED.nodes.registerType("SQL Select",SqlNodeSelect);
    RED.nodes.registerType("SQL Delete",SqlNodeDelete);
};
