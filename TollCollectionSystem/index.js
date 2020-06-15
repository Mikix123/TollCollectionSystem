const Joi = require('@hapi/joi');
const AWS = require('aws-sdk');

const ddb = new AWS.DynamoDB.DocumentClient();
const sqs = new AWS.SQS();

const schema = Joi.object({
    cameraId: Joi.number().integer().required(),
    gateId: Joi.number().integer().required(),
    date: Joi.date().required(),
    car: Joi.required(),
    car: {
        plateNumber: Joi.string().required(),
        make: Joi.string().required(),
        model: Joi.string().required()
    }
});

module.exports.handler = async(event, context) => {
    try {
        await schema.validate(event);
    }
    catch (err) {
        throw new Error('Json message is not Valid ' + err);
    }

    let cars;
    await findCarOwner(event.car.plateNumber)
        .then(res => {
            cars = res.Items;
        })
        .catch(err => { throw new Error('Getting car from db ' + err); });

    if (cars == null || cars.length == 0) {
        console.log('Car owner not found for car plate' + event.car.plateNumber)
        return 'Car owner not found for car plate' + event.car.plateNumber;
    }

    if (cars[0].CarOwner.AccountBalance < process.env.CostOfOneRide) {
        await sendMessageToUser(cars[0].CarOwner, 'Your account balance is to low to open the gate');
        console.log('Account balance to low. Plate number '+ event.car.plateNumber)
        return 'Account balance to low';
    }

    const newAccountBalance = cars[0].CarOwner.AccountBalance - process.env.CostOfOneRide;
    await updateAccountBalance(event.car.plateNumber, newAccountBalance)
        .catch(err => { throw new Error('Updating account balance ' + err); });

    await sendMessageToUser(cars[0].CarOwner, "Your account has been charged for the trip, your current account balance is: " + newAccountBalance);

    await openTheGate(event.cameraId, event.gateId);

    return "Ok";
};

async function updateAccountBalance(plateNumber, newAccountBalance) {

    var params = {
        TableName: "Car",
        Key: {
            "PlateNumber": plateNumber
        },
        UpdateExpression: "set CarOwner.AccountBalance = :r",
        ExpressionAttributeValues: {
            ":r": newAccountBalance
        },
        ReturnValues: "UPDATED_NEW"
    };

    return ddb.update(params).promise();
}

async function findCarOwner(plateNumber) {
    console.log('Finding a car owner for car plate: ', plateNumber);
    const params = {
        KeyConditionExpression: "PlateNumber = :v1",
        ExpressionAttributeValues: {
            ":v1": plateNumber
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

function openTheGate(cameraId, gateId) {

    const messageObj = {
        'cameraId': cameraId,
        'gateId': gateId,
    };

    const params = {
        MessageAttributes: {
            "CameraId": {
                DataType: "Number",
                StringValue: cameraId.toString()
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
	