var mqtt = require('mqtt');
var mongodbURI = 'mongodb://trieule:ttp2018@ds259175.mlab.com:59175/smarthome';
var deviceRoot = "demo/device/";
var collection,mqtt_client;
var mongoose = require('mongoose');

mongoose.Promise = global.Promise;
const connectOptions = { 
  useMongoClient: true,
  autoReconnect: true
};

var db = mongoose.connection;

db.on('connecting', function() 
{
	console.log('connecting to MongoDB...');
});

db.on('error', function(error) 
{
	console.error('Error in MongoDb connection: ' + error);
	mongoose.disconnect();
});

db.on('connected', function() 
{
	console.log('MongoDB connected!');
});

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

db.on('reconnected', function () 
{
	console.log('MongoDB reconnected!');
});

db.on('disconnected', function() 
{
	console.log('MongoDB disconnected!');
	mongoose.connect(mongodbURI, connectOptions);
});

mongoose.connect(mongodbURI, connectOptions);

function payload_paser(topic,message) {
	console.log("Received topic: " + topic);
	console.log("Received message: " + message);
	
	var stringBuf = message.toString('utf-8');
	
	try 
	{
		var myobj3 = JSON.parse(stringBuf);
		db.collection("customers").insertOne(myobj3, function(err, res)
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
		console.log("not JSON");
	}
}

