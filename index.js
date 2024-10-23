// Load environment variables from .env file
import { config } from 'dotenv';
config();

// Import the PostgreSQL client
import { Client } from 'pg';

// Load the Airtable library
import Airtable from 'airtable';

// Initialize Airtable client
const airtableBaseId = process.env.AIRTABLE_BASE_ID;
const airtableApiKey = process.env.AIRTABLE_API_KEY;
const airtableTableName = process.env.AIRTABLE_TABLE_NAME;

const base = new Airtable({ apiKey: airtableApiKey }).base(airtableBaseId);

// Function to process records in batches
async function processRecords() {
    const client = new Client({
        connectionString: process.env.DATABASE_URL,
    });

    try {
        console.log('Connecting to the CockroachDB database...');
        await client.connect();
        console.log('Successfully connected to the database.');

        let offset = 0;
        const limit = 1000;
        let hasMore = true;

        while (hasMore) {
            console.log(`Fetching records with LIMIT ${limit} OFFSET ${offset}...`);
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
            console.log(`Fetched ${records.length} records.`);

            if (records.length === 0) {
                console.log('No more records to process. Exiting loop.');
                hasMore = false;
            } else {
                for (const record of records) {
                    const userId = record.id; // users.id from SQL
                    const userEmail = record.email; // users.email from SQL

                    console.log(`\nProcessing user: ${userEmail} (ID: ${userId})`);

                    try {
                        // Look up the user in Airtable by slack_id
                        console.log(`Looking up Airtable record by slack_id: ${userId}`);
                        let airtableRecords = await base(airtableTableName).select({
                            filterByFormula: `{slack_id} = '${userId}'`,
                            maxRecords: 1
                        }).firstPage();

                        if (airtableRecords.length === 0) {
                            console.log(`No record found with slack_id: ${userId}. Attempting to search by email.`);
                            // If not found, fall back to email
                            airtableRecords = await base(airtableTableName).select({
                                filterByFormula: `{email} = '${userEmail}'`,
                                maxRecords: 1
                            }).firstPage();
                        }

                        if (airtableRecords.length === 0) {
                            // User not found, log and skip
                            console.warn(`User not found in Airtable for slack_id: ${userId} or email: ${userEmail}. Skipping...`);
                            continue;
                        }

                        const airtableRecordId = airtableRecords[0].id;
                        console.log(`Found Airtable record ID: ${airtableRecordId} for user: ${userEmail}`);

                        // Prepare data to update
                        const updateData = {
                            'waka_last_synced_from_db': new Date().toISOString(),
                            'waka_last_heartbeat': record.last_heartbeat,
                            'waka_known_machine_count': Number(record.known_machine_count),
                            'waka_known_machines': JSON.stringify(record.known_machines, null, 2),
                            'waka_30_day_active_machine_count': Number(record["30_day_active_machine_count"]),
                            'waka_30_day_active_machines': JSON.stringify(record["30_day_active_machines"], null, 2),
                            'waka_known_installation_count': Number(record.known_installation_count),
                            'waka_known_installations': JSON.stringify(record.known_installations, null, 2),
                            'waka_30_day_active_installation_count': Number(record["30_day_active_installation_count"]),
                            'waka_30_day_active_installations': JSON.stringify(record["30_day_active_installations"], null, 2),
                        };

                        console.log(`Preparing to update Airtable record for user: ${userEmail} with data:`, updateData);

                        // Update the record in Airtable
                        await base(airtableTableName).update(airtableRecordId, updateData);
                        console.log(`Successfully updated Airtable record for user ${userEmail}`);

                    } catch (error) {
                        console.error(`Error processing user ${userEmail}:`, error);
                    }
                }
                offset += limit;
                console.log(`Incremented offset to ${offset} for next batch.`);
            }
        }

    } catch (err) {
        console.error('Error executing query:', err.stack);
    } finally {
        console.log('Closing database connection.');
        await client.end();
        console.log('Database connection closed.');
    }
}

processRecords();
