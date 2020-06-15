var AWS = require("aws-sdk");

exports.handler = async(event, context, callback) => {

    for (const item of event.Records) {

        console.log("New message " + item.body);
        var result = await sendMessageToOpenTheGate(JSON.parse(item.body));
        callback(null, result);
    }
};

function sendMessageToOpenTheGate(message) {
    
    var iotData = new AWS.IotData({
        endpoint: process.env.iotEndpoint,
        apiVersion: '2015-05-28'
    });
    var params = {
        topic: 'iot/tollCollectionSystem/openTheGate' + message.gateId,
        payload: JSON.stringify(message),
        qos: 0
    };

    console.log(params);
    
    iotData.publish(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data); // successful response
    });
}
