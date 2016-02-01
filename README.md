Node-red Sql Suite
========================


Install
-------

Run the following command in the root directory of your Node-RED install

    npm install node-red-contrib-sql


Overview
-------

This add-on, includes a suite of nodes to work with Sql :

- SQL Insert
- SQL Update
- SQL Select
- SQL Delete

Everyone of these nodes have a param where you specify the params for the connection and the dialect used

## SQL Insert

The parameters for the Insert node are the name of table in which data is inserted.

The ​*msg.payload*​ is the data that will be inserted, this data is filtered using the ​*Accepted columns*​ param, in the case the param is empty - are used all the columns available on ​*msg.payload*​.
Enabling the ​*All columns required ?*​ parameter, if in the ​*msg.payload*​ aren't available all the columns accepted by the node, the query is rejected (This param is ignored in the case the param ​*Accepted columns*​ is empty).

## SQL Update

Like the PG Insert, this node has the parameters table name and accepted column.  These are the columns where the data will be updated. The parameter ​*Where Columns*​ works exactly like the parameter ​*Accepted Columns*​, the values for the where needs to be specified on the ​*msg.where*​

### Example Input

```json
{
   "payload" : {
       "foo" : 1,
       "bar" : "xD",
       "test": 2
   },
   "where" : {
       "foo" : 4
   }
}
```

## SQL Select

Like the ​*PG Update*​ node, this node accept the parameter ​*Where Columns*​, in that case the where clause is specified in the ​*msg.payload*​, in the case the ​*Ignore the where clause ?*​ parameter is enabled, the ​*msg.payload*​ and every time the node is triggered the input isn't checked.

The ​*columns*​ parameter are the columns that you want to select from the table, in the case a ​*msg.group*​ is passed to the node, the ​*Group by*​ parameter is overwritten, same thing for ​*msg.order*​

For each of the ​*limit*​, ​*offset*​ parameters, in the case the value set is different from 0, the ​*msg.limit*​ / ​*msg.offset*​ is ignored

## SQL Delete

Like the ​*PG Select*​ node, this node accept the parameter ​*Where Columns*.

For the ​*limit* parameter, in the case the value set is different from 0, the ​*msg.limit*​ is ignored
