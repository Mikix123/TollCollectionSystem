var AWS = require("aws-sdk");

exports.handler = async(event, context, callback) => {

    for (const item of event.Records) {

        var result = await sendMessageToOpenTheGate(item.body);
        callback(null, result);
    }
};

function sendMessageToOpenTheGate(message) {
    
    var iotData = new AWS.IotData({
        endpoint: 'akugdx70brb.iot.us-west-2.amazonaws.com:8883',
        apiVersion: '2015-05-28'
    });
    var params = {
        topic: 'iot/tollCollectionSystem/openTheGate' + message.gateId,
        payload: message || 'STRING_VALUE',
        qos: 0
    };

    iotData.publish(params, function(err, data) {
        if (err) console.log(err, err.stack); // an error occurred
        else console.log(data); // successful response
    });
}
