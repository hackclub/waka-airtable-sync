// Load environment variables from .env file
import { config } from 'dotenv';
config();

// Import the PostgreSQL client
import { Client } from 'pg';

// Function to process records in batches
async function processRecords() {
    const client = new Client({
        connectionString: process.env.DATABASE_URL,
    });

    try {
        await client.connect();

        let offset = 0;
        const limit = 1000;
        let hasMore = true;

        while (hasMore) {
            const res = await client.query(`
                SELECT
                    users.created_at,
                    users.id,
                    users.name,
                    users.email,
                    MAX(h.time) AS last_heartbeat,
                    COUNT(DISTINCT h.machine) AS known_machine_count,
                    JSON_AGG(DISTINCT h.machine) AS known_machines,
                    COUNT(DISTINCT CASE WHEN h.time >= NOW() - INTERVAL '30 days' THEN h.machine END) AS "30_day_active_machine_count",
                    JSON_AGG(DISTINCT h.machine) FILTER (WHERE h.time >= NOW() - INTERVAL '30 days') AS "30_day_active_machines",
                    COUNT(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) AS known_installation_count,
                    JSON_AGG(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) AS known_installations,
                    COUNT(DISTINCT CASE WHEN h.time >= NOW() - INTERVAL '30 days' THEN CONCAT(h.editor, '|', h.operating_system, '|', h.machine) END) AS "30_day_active_installation_count",
                    JSON_AGG(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) FILTER (WHERE h.time >= NOW() - INTERVAL '30 days') AS "30_day_active_installations"
                FROM
                    users
                JOIN
                    heartbeats h ON users.id = h.user_id
                GROUP BY
                    users.id
                ORDER BY
                    users.created_at ASC
                LIMIT $1 OFFSET $2;
            `, [limit, offset]);

            const records = res.rows;

            if (records.length === 0) {
                hasMore = false;
            } else {
                for (const record of records) {
                    console.log(record)
                    // Place your code here to process each record
                    // For example:
                    // console.log(record);
                }
                offset += limit;
            }
        }

    } catch (err) {
        console.error('Error executing query', err.stack);
    } finally {
        await client.end();
    }
}

processRecords();

