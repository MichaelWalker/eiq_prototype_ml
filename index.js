import {} from "dotenv/config";
import knex from "knex";

const jobName = process.env.JOB_NAME;

const db = knex({
        client: "pg",
        connection: process.env.DATABASE_CONNECTION_STRING
    }
);

async function startTask() {
    const inserted = await db
        .insert({ 
            name: jobName,
            status: "RUNNING",
        })
        .into("task")
        .returning("*");
    return inserted[0].id;
}

async function completeTask(id) {
    console.log(`Completed job ${jobName} with id ${id}`);
    await db
        .update({
            status: "COMPLETED"
        })
        .from("task")
        .where("id", id);
}

async function failTask(id, error) {
    console.error(`Job ${jobName} with id ${id} failed: ${error}`);
    await db
        .update({
            status: "FAILED"
        })
        .from("task")
        .where("id", id);
}

function quickTask() {
    return new Promise((resolve, reject) => {
        if (Math.random() > 0.8) {
            return reject("Oh no - something went wrong...");
        }
        return resolve();
    });
}

function slowTask() {
    return new Promise((resolve, reject) => {
        setTimeout(() => {
            if (Math.random() > 0.8) {
                return reject("Oh no - something went wrong...");
            }
            return resolve();
        }, 120000)
    });
}

async function processTask() {
    const taskId = await startTask();
    
    try {
        const task = jobName === "slow_task" ? slowTask : quickTask;
        await task();
        await completeTask(taskId);
    } catch (error) {
        await failTask(taskId, error);
        throw Error(error);
    }

}

console.log(`starting job: ${jobName}`);
processTask()
    .then(() => console.log("task completed successfully"))
    .catch((error) => console.log("task failed", error))
    .finally(() => {
        console.log(`shutting down.`); 
        process.exit(0);
    })

