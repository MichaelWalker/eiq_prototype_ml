import {} from "dotenv/config";
import knex from "knex";
import aws from "aws-sdk";

const db = knex({
        client: "pg",
        connection: process.env.DATABASE_CONNECTION_STRING
    }
);

async function changeTaskStatus(taskId, name, newStatus) {

    if (taskId) {
        console.log("found existing task");
        return await db
            .update({ 
                status: newStatus
            })
            .from("task")
            .where("id", taskId)
            .returning("*");
    } else {
        return await db
            .insert({ 
                name: name,
                status: newStatus,
            })
            .into("task")
            .returning("*");
    }
}

async function processMessage(message) {
    const { id, name } = JSON.parse(message.Body);
    const task = await changeTaskStatus(id, name, "IN_PROGRESS");
    
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            changeTaskStatus(task.id, task.name, "COMPLETED")
                .then(() => resolve());
        }, 30000);
    });
}

console.log("creating sqs client");
const sqs = new aws.SQS({ 
    region: "eu-west-2",
    credentials: {
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    }  
});

const receiveParams = {
    QueueUrl: process.env.QUEUE_URL,
    MaxNumberOfMessages: 1,
    WaitTimeSeconds: 10,
};

console.log("fetching message from sqs");
sqs.receiveMessage(receiveParams, async (error, messages) => {
    if (error) {
        console.error(error);
        return;
    }

    if (messages.length === 0) {
        console.error("No data returned");
        return;
    }

    console.log("found message", messages[0]);
    await processMessage(messages[0]);
    console.log("processed the message.");
    sqs.deleteMessage({
        QueueUrl: process.env.QUEUE_URL,
        ReceiptHandle: messages[0].ReceiptHandle
    });
    console.log("done! :)");
});