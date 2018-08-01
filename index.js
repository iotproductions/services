/*
	MQTT Services
	Author: Trieu Le
	Created Date: 31/07/2018
	Description: This is simple service for receiving MQTT payload (JSON Format) and stored it into MongoDB
*/
// Requires libraries 
var mqtt = require('mqtt');
var mongodbURI = 'mongodb://trieule:ttp2018@ds259175.mlab.com:59175/smarthome';
var deviceRoot = "demo/device/";
var collection,mqtt_client;
var mongoose = require('mongoose');

mongoose.Promise = global.Promise;
// MongoDB connection options
const connectOptions = { 
  useMongoClient: true,
  autoReconnect: true
};
// Create MongoDB connection
var db = mongoose.connection;
// Connecting to MongoDB 
db.on('connecting', function() 
{
	console.log('connecting to MongoDB...');
});
// Error issues during connect to MongoDB 
db.on('error', function(error) 
{
	console.error('Error in MongoDb connection: ' + error);
	mongoose.disconnect();
});
// Connected to MongoDB 
db.on('connected', function() 
{
	console.log('MongoDB connected!');
});
// Opened MongoDB connection success
db.once('open', function() 
{
	console.log('MongoDB connection opened!');
	//----------------------------------------------------------------------------------
	// MQTT Service
	//----------------------------------------------------------------------------------
	// MQTT Client config
	mqtt_client = mqtt.connect(
	{ 	
		host: 'localhost', 
		port: 1883, 
		keepalive: 60000,
		username: 'trieu.le',
		password: 'trieu.le',
		protocolId: 'MQIsdp',
		protocolVersion: 3
	});
	// MQTT Connect to Broker
	mqtt_client.on('connect', function()
	{
		console.log('Connected to Broker');
	});
	// Subcribe all topic
	mqtt_client.subscribe('#');
	// MQTT Incomming message parser
	console.log("Connected to MQTT Broker !");
	//mqtt_client.subscribe(deviceRoot+"+");
	mqtt_client.on('message', payload_paser);
	// MQTT Close Handler
	mqtt_client.on('close', function () 
	{
	  // Reconnect to Broker
	  mqtt_client = mqtt.connect(
		{ 	
			host: 'localhost', 
			port: 1883, 
			keepalive: 60000,
			username: 'trieu.le',
			password: 'trieu.le',
			protocolId: 'MQIsdp',
			protocolVersion: 3
		});
	});
});
// Reconnecting to MongoDB 
db.on('reconnected', function () 
{
	console.log('MongoDB reconnected!');
});
// Disconnected to MongoDB 
db.on('disconnected', function() 
{
	console.log('MongoDB disconnected!');
	mongoose.connect(mongodbURI, connectOptions);
});
// Connecting to MongoDB via URI with options
mongoose.connect(mongodbURI, connectOptions);
// Pasing MQTT Payload
function payload_paser(topic,message) {
	console.log("Received topic: " + topic);
	console.log("Received message: " + message);
	// Convert payload to String
	var stringBuf = message.toString('utf-8');
	
	try 
	{
		// Parser JSON Object
		var jsonData = JSON.parse(stringBuf);
		console.log("jsonData: " + jsonData);
		try
		{
			console.log("jsonData.status: " + jsonData.status);
			if(jsonData.status == get)
			{
				var publish_options = 
				{
					  retain:false,
					  qos: 1
				};	
				client.publish('SERVICE_STATUS', "OK",publish_options);
				console.log("Publish message to topic SERVICE_STATUS");
			}
		}
		catch (e)
		{
			console.log("[Error] Payload is not a JSON object !");
		}
		// Save data into MongoDB
		db.collection("sensor_datas").insertOne(jsonData, function(err, res)
		{
			if (err) 
			{
				console.log("err: ",err);
				throw err;
			}
			console.log("1 record inserted");
			//db.close();
		});
	} 
	catch (e)
	{
		console.log("[Error] Payload is not a JSON object !");
	}
}

