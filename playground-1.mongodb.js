/* global use, db */
// MongoDB Playground
// To disable this template go to Settings | MongoDB | Use Default Template For Playground.
// Make sure you are connected to enable completions and to be able to run a playground.
// Use Ctrl+Space inside a snippet or a string literal to trigger completions.
// The result of the last command run in a playground is shown on the results panel.
// By default the first 20 documents will be returned with a cursor.
// Use 'console.log()' to print to the debug output.
// For more documentation on playgrounds please refer to
// https://www.mongodb.com/docs/mongodb-vscode/playgrounds/

// Select the database to use.
// use('mongodbVSCodePlaygroundDB');
// use('nse_historical');
// db.getCollection('stock_options')
// .find({'Date':ISODate('2023-04-28T00:00:00.000+00:00')})
// .count()
use('nse_historical');
// db.getCollection('atm_stock_options')
// .deleteMany({'Date':ISODate('2023-04-25T00:00:00.000+00:00')})
//deleteMany({'Date':{$gte:ISODate('2023-04-25T00:00:00.000+00:00')}})
//  db.getCollection('stock_futures').deleteMany({'Date':{$gt:ISODate('2023-04-12T00:00:00.000+00:00')}})
//  db.getCollection('activity').updateMany({   "instrument": "opt"},{
//     $set:{"last_accessed_date": "2023-06-08T00:00:00.000Z"}
//  })
db.getCollection('stocks_step').find({Symbol:'ONGC'})
// db.getCollection('options_straddle').find({'Date':ISODate('2023-05-12T00:00:00.000+00:00')  })
// db.getCollection('options_straddle').aggregate(
// [
//    {'$match': {'Date':ISODate('2023-05-12T00:00:00.000+00:00') , 'strike': {'$lt': 7000}, 'two_months_week_min_coverage': {'$ne':'nan'}, 'current_vs_prev_two_months': {'$gte': -5, '$lte': 0}}}, {'$group': {'_id': {'symbol': '$symbol', 'Date': '$Date', 'Expiry': '$Expiry', '%coverage': '$%coverage', 'two_months_week_min_coverage': '$two_months_week_min_coverage', 'current_vs_prev_two_months': '$current_vs_prev_two_months', 'strike': '$strike', 'straddle_premium': '$straddle_premium', 'week_min_coverage': '$week_min_coverage', 'weeks_to_expiry': '$weeks_to_expiry', 'days_to_expiry': '$days_to_expiry'}, 'distinct_val': {'$addToSet': '$Date'}}}, {'$unwind': {'path': '$distinct_val', 'preserveNullAndEmptyArrays': true}}, {'$project': {'symbol': '$_id.symbol', 'Date': '$_id.Date', '%coverage': '$_id.%coverage', 'two_months_week_min_coverage': '$_id.two_months_week_min_coverage', 'current_vs_prev_two_months': '$_id.current_vs_prev_two_months', 'strike': '$_id.strike', 'straddle_premium': '$_id.straddle_premium', 'week_min_coverage': '$_id.week_min_coverage', 'weeks_to_expiry': '$_id.weeks_to_expiry', 'days_to_expiry': '$_id.days_to_expiry', 'expiry': '$_id.Expiry', '_id': 0}}, {'$sort': {'current_vs_prev_two_months': 1}}, {'$limit': 15}])
// db.getCollection('stocks_step').findOne({Symbol:"BEL"})
db.getCollection('closed_positions').deleteMany({ })
// db.getCollection('atm_stock_options').find(
//     {
//       Date: ISODate('2023-05-27T00:00:00.000+00:00')
//       // 'symbol':'TATACONSUM',
//     // "weeks_to_expiry":"week5",

//     // 'Expiry':{
//     //     "$gte":ISODate('2023-03-31T00:00:00.000+00:00'),
//     //     "$lt":ISODate('2023-05-25T00:00:00.000+00:00')
//     // }
// }).count()
/*
const find_cheapest_pipeline=[
  {'$match': 
  {'Date':  ISODate('2023-05-27T00:00:00.000+00:00'),
   'strike': {'$lt': 7000}, 
   'two_months_week_min_coverage': {'$ne': null}, 
  //  'current_vs_prev_two_months': {'$gte': -5, '$lte': 0}
  }
},
{'$group': 
{'_id':
 {'symbol': '$symbol',
  'Date': '$Date', 
  'Expiry': '$Expiry', 
  '%coverage': '$%coverage', 
  'two_months_week_min_coverage': '$two_months_week_min_coverage',
   'current_vs_prev_two_months': '$current_vs_prev_two_months',
    'strike': '$strike', 
    'straddle_premium': '$straddle_premium',
     'week_min_coverage': '$week_min_coverage', 
     'weeks_to_expiry': '$weeks_to_expiry', 
     'days_to_expiry': '$days_to_expiry'
    }, 
    'distinct_val': {'$addToSet': '$Date'}}},
     {'$unwind':
      {'path': '$distinct_val', 'preserveNullAndEmptyArrays': true}},
       {'$project':
        {'symbol': '$_id.symbol', 
        'Date': '$_id.Date', '%coverage': '$_id.%coverage', 
        'two_months_week_min_coverage': '$_id.two_months_week_min_coverage', 
        'current_vs_prev_two_months': '$_id.current_vs_prev_two_months',
         'strike': '$_id.strike', 'straddle_premium': '$_id.straddle_premium', 
         'week_min_coverage': '$_id.week_min_coverage', 
         'weeks_to_expiry': '$_id.weeks_to_expiry', 'days_to_expiry': '$_id.days_to_expiry', 
         'expiry': '$_id.Expiry', '_id': 0}},
          {'$sort': {'current_vs_prev_two_months': 1}}, 
        {'$limit': 15}]
db.getCollection('options_straddle').aggregate(find_cheapest_pipeline)
*/
// db.getCollection('stock_futures').find({Date:ISODate('2022-07-03T00:00:00.000+00:00')})

// const pipeline=[{
// "$match":{
//     'Expiry':{
//         "$gte":ISODate('2023-03-31T00:00:00.000+00:00'),
//         "$lt":ISODate('2023-05-25T00:00:00.000+00:00')
//     }
// }
// },
//     {

//         '$group': {
//                     '_id': {
//                         'symbol': '$Symbol',
//                         'Date': '$Date',
//                         'strike_price': '$Strike Price',
//                         'Expiry': '$Expiry',
//                         'days_to_expiry': '$days_to_expiry',
//                         'fut_close': '$fut_close',
//                         'option_type': '$Option Type',
//                         'close': '$Close'
//                     },
//         },
//     },
//     {
//         '$group': {
//             '_id': {
//                 'symbol': '$_id.symbol',
//                 'Date': '$_id.Date',
//                 'strike_price': '$_id.strike_price',
//                 'Expiry': '$_id.Expiry',
//                 'days_to_expiry': '$_id.days_to_expiry',
//                 'fut_close': '$_id.fut_close',
//             },
//             'premiums': {
//                 '$push': '$_id.close'
//             },
//             'option_types': {
//                 '$addToSet': '$_id.option_type'
//             }
//         }
//     },

//     {
//         '$project': {
//             'symbol': '$_id.symbol',
//             'premiums': '$premiums',
//             'strike': '$_id.strike_price',
//             'Date': '$_id.Date',
//             'Expiry': '$_id.Expiry',
//             'days_to_expiry': '$_id.days_to_expiry',
       
//             'fut_close': {'$toDouble':'$_id.fut_close'},
//             'straddle_premium': {
//                 '$sum': '$premiums'
//             },
//             '_id': 0
//         }
//     },
//     {
//         '$project': {
//             'symbol': '$symbol',
//             'premiums': '$premiums',
//             'strike': '$strike',
//             'Date': '$Date',
//             'Expiry': '$Expiry',
//             'days_to_expiry': '$days_to_expiry',
//             'straddle_premium': '$straddle_premium',
//             '%coverage': {
//                 '$multiply': [
//                     {'$divide': ['$straddle_premium', '$fut_close']},
//                     100
//                 ]
//             }
//         }
//     },
//     {
//         "$sort":{
//             "Date":1
//         }
//     }
// ]
// db.getCollection('atm_stock_options').aggregate(pipeline)
// db.getCollection('atm_stock_options').aggregate([{
//     "$match":{
//         'Symbol':'TATACONSUM',
//         'Expiry':{
//             "$gte":ISODate('2023-03-31T00:00:00.000+00:00'),
//             "$lt":ISODate('2023-05-25T00:00:00.000+00:00')
//         }
//     }
// }
// ,
// {
//     "$group":{
//         "_id":{weeks_to_expiry:"$weeks_to_expiry"},

//     }
// }
// ]
// )
// db.getCollection('historical_options').find({'Symbol':'SBILIFE','Date':ISODate('2023-05-24T00:00:00.000+00:00')})
// const pipeline=[
//     {
//       $match: {

//         Date: {
//           $gte: ISODate('2022-05-26T00:00:00.000+00:00'),
//           $lte: ISODate('2022-07-28T00:00:00.000+00:00')
//         }
//       }
//     },
//     {
//       $group: {
//         _id: {dayOfMonth:{ $dayOfMonth: "$Date"},  Expiry: "$Expiry",Date:"$Date"},
//         count: { $sum: 1 }
//       }
//     },
//     {
//       $project: {
//         _id: 0,
//         dayOfMonth: "$_id.dayOfMonth",
//         Expiry:"$_id.Expiry",
//         Date:"$_id.Date",
//         count: 1
//       }
//     },
//     {
//         $sort: {
//             Date: 1
//         }
//     }
//   ]
  // db.getCollection('atm_stock_options').deleteMany({
  //   Date: {
  //     $gte: ISODate('2023-06-08T00:00:00.000+00:00'),
  //     $lte: ISODate('2023-07-28T00:00:00.000+00:00')
  //   }
  // })
  // db.getCollection('atm_stock_options').aggregate(pipeline)
//  db.getCollection('atm_stock_options').aggregate([
//         {
//               $match: {
        
//                 Date: {
//                   $gte: ISODate('2023-04-28T00:00:00.000+00:00'),
//                   $lte: ISODate('2023-04-29T00:00:00.000+00:00')
//                 }
//               }
//             },
//     {
//       $group: {
//         _id: { date: '$Date', Symbol: '$Symbol'},
//         ids: { $push: '$_id' },
       
//         count: { $sum: 1 }
//       }
//     },
//     {
//       $match: {
//         count: { $gt: 2 } // Keep only duplicates
//       }
//     }
//   ])
// db.getCollection('atm_stock_options').aggregate([
//     {$match: {
      
//         Date: {
//           $gte: ISODate('2023-04-28T00:00:00.000+00:00'),
//           $lt: ISODate('2023-04-29T00:00:00.000+00:00')
//         }
//       }
//     },
//     {          
//       $group: {
//         _id: { date: "$Date", symbol: "$Symbol" },
//         latest: { $max: "$_id" }
//       }
//     },
//     {
//       $project: { _id: "$latest" }
//     }
//   ])

//   db.getCollection('atm_stock_options').remove({
//     _id: {
//       $nin: db.getCollection('atm_stock_options').aggregate([
//         {$match: {
          
//             Date: {
//               $gte: ISODate('2023-04-28T00:00:00.000+00:00'),
//               $lt: ISODate('2023-04-29T00:00:00.000+00:00')
//             }
//           }
//         },
//         {          
//           $group: {
//             _id: { date: "$Date", symbol: "$Symbol" },
//             latest: { $max: "$_id" }
//           }
//         },
//         {
//           $project: { _id: "$latest" }
//         }
//       ]).map(result => result._id)
//     }
//   });
  
  
//  db.getCollection('stock_futures').deleteMany({'Date':ISODate('2023-05-12T00:00:00.000+00:00')  })
// db.getCollection('stock_futures').find({'Date':ISODate('2023-05-15T00:00:00.000+00:00')}).count()
// db.getCollection('atm_stock_options').find({'Date':ISODate('2023-05-12T00:00:00.000+00:00')}).count()
// db.getCollection('options_straddle').find().count()
// db.getCollection('atm_stock_options').find({'Expiry': ISODate('2023-05-25T00:00:00.000+00:00')},{Date:1,_id:0}).sort({Date:1})
// db.getCollection('orders').deleteMany({})
// db.getCollection('atm_stock_options').find({deleted:true}).count()
// db.atm_stock_options.find({ "deleted": true }).forEach(printjson);
//  db.getCollection('options_data').find({'Symbol': 'HONAUT', 'Strike Price': 37000.0}).sort({Date:-1}).limit(2)

// db.getCollection('stock_futures')
// .find({
//    'Date':ISODate('2023-04-27T00:00:00.000+00:00'),
//    Expiry: ISODate('2023-05-25T00:00:00.000+00:00')
// }).count()

// // Insert a few documents into the sales collection.
// db.getCollection('sales').insertMany([
//   { 'item': 'abc', 'price': 10, 'quantity': 2, 'date': new Date('2014-03-01T08:00:00Z') },
//   { 'item': 'jkl', 'price': 20, 'quantity': 1, 'date': new Date('2014-03-01T09:00:00Z') },
//   { 'item': 'xyz', 'price': 5, 'quantity': 10, 'date': new Date('2014-03-15T09:00:00Z') },
//   { 'item': 'xyz', 'price': 5, 'quantity': 20, 'date': new Date('2014-04-04T11:21:39.736Z') },
//   { 'item': 'abc', 'price': 10, 'quantity': 10, 'date': new Date('2014-04-04T21:23:13.331Z') },
//   { 'item': 'def', 'price': 7.5, 'quantity': 5, 'date': new Date('2015-06-04T05:08:13Z') },
//   { 'item': 'def', 'price': 7.5, 'quantity': 10, 'date': new Date('2015-09-10T08:43:00Z') },
//   { 'item': 'abc', 'price': 10, 'quantity': 5, 'date': new Date('2016-02-06T20:20:13Z') },
// ]);

// // Run a find command to view items sold on April 4th, 2014.
// const salesOnApril4th = db.getCollection('sales').find({
//   date: { $gte: new Date('2014-04-04'), $lt: new Date('2014-04-05') }
// }).count();

// // Print a message to the output window.
// console.log(`${salesOnApril4th} sales occurred in 2014.`);

// // Here we run an aggregation and open a cursor to the results.
// // Use '.toArray()' to exhaust the cursor to return the whole result set.
// // You can use '.hasNext()/.next()' to iterate through the cursor page by page.
// db.getCollection('sales').aggregate([
//   // Find all of the sales that occurred in 2014.
//   { $match: { date: { $gte: new Date('2014-01-01'), $lt: new Date('2015-01-01') } } },
//   // Group the total sales for each product.
//   { $group: { _id: '$item', totalSaleAmount: { $sum: { $multiply: [ '$price', '$quantity' ] } } } }
// ]);
