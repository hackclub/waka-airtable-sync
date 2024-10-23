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

// Initialize an array to hold batched updates
let updateBatch = [];

// Initialize a flag to track Airtable errors
let airtableErrorOccurred = false;

// Function to sanitize and serialize array fields
function sanitizeAndSerialize(array) {
    if (Array.isArray(array)) {
        // Filter out items that are empty strings, null, undefined, or contain only '|'
        const filtered = array.filter(item => 
            item && 
            item.trim() !== '' && 
            item.replace(/\|/g, '').trim() !== ''
        );
        return filtered.length > 0 ? JSON.stringify(filtered, null, 2) : null;
    }
    return null;
}

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
                WITH time_logged AS (
                    SELECT
                        user_id,
                        ROUND(SUM(GREATEST(1, diff)) / 3600.0, 2) as total_hours,
                        ROUND(SUM(GREATEST(1, diff)) FILTER (WHERE time >= NOW() - INTERVAL '30 days') / 3600.0, 2) as thirty_day_hours
                    FROM (
                        SELECT
                            user_id,
                            time,
                            EXTRACT(EPOCH FROM LEAST(time - LAG(time) OVER w, INTERVAL '2 minutes')) as diff
                        FROM heartbeats
                        WINDOW w AS (PARTITION BY user_id ORDER BY time)
                    ) s
                    WHERE diff IS NOT NULL
                    GROUP BY user_id
                )
                SELECT
                    users.created_at,
                    users.id,
                    users.name,
                    users.email,
                    MIN(h.time) AS first_heartbeat,
                    MAX(h.time) AS last_heartbeat,
                    COUNT(DISTINCT h.machine) AS known_machine_count,
                    JSON_AGG(DISTINCT h.machine) AS known_machines,
                    COUNT(DISTINCT CASE WHEN h.time >= NOW() - INTERVAL '30 days' THEN h.machine END) AS "30_day_active_machine_count",
                    JSON_AGG(DISTINCT h.machine) FILTER (WHERE h.time >= NOW() - INTERVAL '30 days') AS "30_day_active_machines",
                    COUNT(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) AS known_installation_count,
                    JSON_AGG(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) AS known_installations,
                    COUNT(DISTINCT CASE WHEN h.time >= NOW() - INTERVAL '30 days' THEN CONCAT(h.editor, '|', h.operating_system, '|', h.machine) END) AS "30_day_active_installation_count",
                    JSON_AGG(DISTINCT CONCAT(h.editor, '|', h.operating_system, '|', h.machine)) FILTER (WHERE h.time >= NOW() - INTERVAL '30 days') AS "30_day_active_installations",
                    COALESCE(tl.total_hours, 0) AS total_hours_logged,
                    COALESCE(tl.thirty_day_hours, 0) AS "30_day_hours_logged"
                FROM
                    users
                LEFT JOIN
                    heartbeats h ON users.id = h.user_id
                LEFT JOIN
                    time_logged tl ON users.id = tl.user_id
                GROUP BY
                    users.id, tl.total_hours, tl.thirty_day_hours
                ORDER BY
                    users.created_at ASC
                LIMIT $1 OFFSET $2;
            `, [limit, offset]);

            const records = res.rows;
            console.log(`Fetched ${records.length} records.`);

            if (records.length < limit) {  
                console.log('No more records to process. Exiting loop.');
                hasMore = false;
            }

            if (records.length > 0) {
                for (const record of records) {
                    const userId = record.id; // users.id from SQL
                    const userEmail = record.email; // users.email from SQL

                    console.log(`\nProcessing user: ${userEmail} (ID: ${userId})`);

                    try {
                        // Look up the user in Airtable by slack_id
                        console.log(`Looking up Airtable record by slack_id: ${userId}`);
                        let airtableRecords = await base(airtableTableName).select({
                            filterByFormula: `{slack_id} = '${userId.replace(/'/g, "''")}'`,
                            maxRecords: 1
                        }).firstPage();

                        if (airtableRecords.length === 0) {
                            console.log(`No record found with slack_id: ${userId}. Attempting to search by email.`);
                            // If not found, fall back to email
                            airtableRecords = await base(airtableTableName).select({
                                filterByFormula: `{email} = '${userEmail.replace(/'/g, "''")}'`,
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
                            'waka_first_heartbeat': record.first_heartbeat || null,
                            'waka_last_heartbeat': record.last_heartbeat || null,
                            'waka_known_machine_count': Number(record.known_machine_count) === 0 ? null : Number(record.known_machine_count),
                            'waka_known_machines': sanitizeAndSerialize(record.known_machines),
                            'waka_30_day_active_machine_count': Number(record["30_day_active_machine_count"]) === 0 ? null : Number(record["30_day_active_machine_count"]),
                            'waka_30_day_active_machines': sanitizeAndSerialize(record["30_day_active_machines"]),
                            'waka_known_installation_count': Number(record.known_installation_count) === 0 ? null : Number(record.known_installation_count),
                            'waka_known_installations': sanitizeAndSerialize(record.known_installations),
                            'waka_30_day_active_installation_count': Number(record["30_day_active_installation_count"]) === 0 ? null : Number(record["30_day_active_installation_count"]),
                            'waka_30_day_active_installations': sanitizeAndSerialize(record["30_day_active_installations"]),
                            'waka_total_hours_logged': Number(record.total_hours_logged) === 0 ? null : Number(record.total_hours_logged),
                            'waka_30_day_hours_logged': Number(record["30_day_hours_logged"]) === 0 ? null : Number(record["30_day_hours_logged"]),
                        };

                        console.log(`Preparing to update Airtable record for user: ${userEmail} with data:`, updateData);

                        // Add the update to the batch
                        updateBatch.push({
                            id: airtableRecordId,
                            fields: updateData,
                        });

                        // Batch update when reaching 10 records
                        if (updateBatch.length === 10) {
                            console.log(`Updating Airtable with batch of ${updateBatch.length} records...`);
                            try {
                                await base(airtableTableName).update(updateBatch);
                                console.log(`Batch update successful.`);
                            } catch (error) {
                                console.error('Error during batch update:', error);
                                airtableErrorOccurred = true;
                            }
                            updateBatch = [];
                        }

                        console.log(`Successfully updated Airtable record for user ${userEmail}`);

                    } catch (error) {
                        console.error(`Error processing user ${userEmail}:`, error);
                    }
                }

                // Only increment offset if the current batch was full
                if (records.length === limit) {  
                    offset += limit;
                    console.log(`Incremented offset to ${offset} for next batch.`);
                }
            }
        }

        // After processing all records, send any remaining updates
        if (updateBatch.length > 0) {
            console.log(`Updating Airtable with final batch of ${updateBatch.length} records...`);
            try {
                await base(airtableTableName).update(updateBatch);
                console.log(`Final batch update successful.`);
            } catch (error) {
                console.error('Error during final batch update:', error);
                airtableErrorOccurred = true;
            }
        }

        // Determine exit status based on Airtable errors
        if (airtableErrorOccurred) {
            console.log('One or more Airtable errors occurred during processing.');
            process.exit(1);
        } else {
            console.log('All Airtable updates completed successfully.');
            process.exit(0);
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
