const Joi = require('joi');
const AWS = require('aws-sdk');

const ddb = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();

const schema = Joi.object({
    deviceId: Joi.number().integer().required(),
    gateId: Joi.number().integer().required(),
    date: Joi.date().required(),
    car: Joi.required(),
    car: {
        plateNumer: Joi.string().required(),
        make: Joi.string().required(),
        model: Joi.string().required()
    }
});

exports.handler = async(event, context) => {
    try {
        await schema.validate(event);
    }
    catch (err) {
        throw new Error('Json message is not Valid ' + err);
    }

    let cars;
    await findCarOwner(event.car.plateNumer)
        .then(res => {
            cars = res.Items;
        })
        .catch(err => { throw new Error('Getting car from db ' + err); });

    if (cars == null || cars.length == 0) {
        return "Car owner not found for car plate" + event.car.plateNumer;
    }

    if (cars[0].CarOwner.AccountBalance < process.env.CostOfOneRide) {
        await sendMessageToUser(cars[0].CarOwner, "Your account balance is to low to open the gate");
        return "Account balance to low";
    }

    const newAccountBalance = cars[0].CarOwner.AccountBalance - process.env.CostOfOneRide;
    await updateAccountBalance(event.car.plateNumer, newAccountBalance)
        .catch(err => { throw new Error('Updating account balance ' + err); });

    await sendMessageToUser(cars[0].CarOwner, "Your account has been charged for the trip, your current account balance is: " + newAccountBalance);

    await openTheGate(event.deviceId, event.gateId);

    return "Ok";
};

async function updateAccountBalance(plateNumer, newAccountBalance) {

    var params = {
        TableName: "Car",
        Key: {
            "PlateNumer": plateNumer
        },
        UpdateExpression: "set CarOwner.AccountBalance = :r",
        ExpressionAttributeValues: {
            ":r": newAccountBalance
        },
        ReturnValues: "UPDATED_NEW"
    };

    return ddb.update(params).promise();
}

async function findCarOwner(plateNumer) {
    console.log('Finding a car owner for car plate: ', plateNumer);
    const params = {
        KeyConditionExpression: "PlateNumer = :v1",
        ExpressionAttributeValues: {
            ":v1": plateNumer
        },
        TableName: "Car"
    };
    return ddb.query(params).promise();
}

async function sendMessageToUser(owner, message) {

    const messageObj = {
        'userEmail': owner.Email,
        'message': message,
    }

    const params = {
        MessageAttributes: {
            "UserEmail": {
                DataType: "String",
                StringValue: owner.Email
            },
            "Message": {
                DataType: "String",
                StringValue: message
            }
        },
        MessageBody: JSON.stringify(messageObj),
        QueueUrl: process.env.TollCollectionSystemSendMessageToUser
    };
    return sqs.sendMessage(params).promise();
}

function openTheGate(deviceId, gateId) {

    const messageObj = {
        'deviceId': deviceId,
        'gateId': gateId,
    };

    const params = {
        MessageAttributes: {
            "DeviceId": {
                DataType: "Number",
                StringValue: deviceId.toString()
            },
            "GateId": {
                DataType: "Number",
                StringValue: gateId.toString()
            }
        },
        MessageBody: JSON.stringify(messageObj),
        QueueUrl: process.env.TollCollectionSystemOpenGate
    };
    return sqs.sendMessage(params).promise();
}
