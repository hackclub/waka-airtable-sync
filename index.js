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

// Function to build OR formula for Airtable filterByFormula
function buildOrFormula(fieldName, values) {
    const escapedValues = values.map(value => `'${value.replace(/'/g, "''")}'`);
    const conditions = escapedValues.map(value => `{${fieldName}} = ${value}`);
    return `OR(${conditions.join(', ')})`;
}

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

// Initialize a Map to hold batched updates, using record IDs as keys
let updateBatch = new Map();

// Initialize a flag to track Airtable errors
let airtableErrorOccurred = false;

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
        const limit = 2500;
        let hasMore = true;

        let emailHeartbeatMap = {}; // To track emails and their latest heartbeat
        let userLookupBatch = [];   // To batch user IDs and emails for lookup

        while (hasMore) {
            console.log(`Fetching records with LIMIT ${limit} OFFSET ${offset}...`);
            const res = await client.query(`
                WITH high_seas_heartbeats AS (
                    SELECT *
                    FROM heartbeats
                    WHERE time >= '2024-10-29 10:00' AND time <= '2025-02-01 12:00'
                ),
                time_logged AS (
                    SELECT
                        user_id,
                        ROUND(SUM(GREATEST(1, diff)) / 3600.0, 2) as total_hours,
                        ROUND(SUM(GREATEST(1, diff)) FILTER (WHERE time >= NOW() - INTERVAL '30 days') / 3600.0, 2) as thirty_day_hours
                    FROM (
                        SELECT
                            user_id,
                            time,
                            EXTRACT(EPOCH FROM LEAST(time - LAG(time) OVER w, INTERVAL '2 minutes')) as diff
                        FROM high_seas_heartbeats
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
                    high_seas_heartbeats h ON users.id = h.user_id
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
                    const userId = record.id;
                    const userEmail = record.email;
                    const lastHeartbeat = record.last_heartbeat;

                    // Keep track of the latest heartbeat per email
                    if (!emailHeartbeatMap[userEmail] || lastHeartbeat > emailHeartbeatMap[userEmail].lastHeartbeat) {
                        emailHeartbeatMap[userEmail] = {
                            userId,
                            lastHeartbeat,
                            record,
                        };
                    }

                    // Collect user IDs and emails for batch lookup
                    userLookupBatch.push({ userId, userEmail });

                    // If batch size reaches 10, perform lookup
                    if (userLookupBatch.length === 10) {
                        await lookupAndUpdateUsers(userLookupBatch, emailHeartbeatMap);
                        userLookupBatch = [];
                    }
                }

                // Process any remaining users in the batch
                if (userLookupBatch.length > 0) {
                    await lookupAndUpdateUsers(userLookupBatch, emailHeartbeatMap);
                    userLookupBatch = [];
                }

                // Only increment offset if the current batch was full
                if (records.length === limit) {  
                    offset += limit;
                    console.log(`Incremented offset to ${offset} for next batch.`);
                }
            }
        }

        // After processing all records, send any remaining updates
        if (updateBatch.size > 0) {
            console.log(`Updating Airtable with final batch of ${updateBatch.size} records...`);
            try {
                await base(airtableTableName).update(Array.from(updateBatch.values()));
                console.log(`Final batch update successful.`);
            } catch (error) {
                console.error('Error during final batch update:', error);
                airtableErrorOccurred = true;
            }
            updateBatch.clear();
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

// Function to lookup and update users in batches
async function lookupAndUpdateUsers(userBatch, emailHeartbeatMap) {
    try {
        // Build filterByFormula using slack_id and email
        const userIds = userBatch.map(u => u.userId);
        const userEmails = userBatch.map(u => u.userEmail);

        const slackIdFormula = buildOrFormula('slack_id', userIds);
        const emailFormula = buildOrFormula('email', userEmails);

        // Combine formulas with OR
        const filterFormula = `OR(${slackIdFormula}, ${emailFormula})`;

        // Perform Airtable lookup
        console.log(`Looking up Airtable records for batch of users...`);
        const airtableRecords = await base(airtableTableName).select({
            filterByFormula: filterFormula,
        }).all();

        // Map Airtable records by slack_id and email
        const airtableRecordMap = {};
        airtableRecords.forEach(record => {
            const slackId = record.get('slack_id');
            const email = record.get('email');
            if (slackId) {
                airtableRecordMap[slackId] = record;
            }
            if (email) {
                airtableRecordMap[email] = record;
            }
        });

        // Prepare updates
        for (const user of userBatch) {
            const { userId, userEmail } = user;
            const emailEntry = emailHeartbeatMap[userEmail];

            // Skip if this is not the user with the latest heartbeat for this email
            if (userId !== emailEntry.userId) {
                console.log(`Skipping user ${userEmail} (ID: ${userId}) in favor of user ID: ${emailEntry.userId} with more recent heartbeat.`);
                continue;
            }

            // Find Airtable record
            let airtableRecord = airtableRecordMap[userId] || airtableRecordMap[userEmail];
            if (!airtableRecord) {
                console.warn(`User not found in Airtable for slack_id: ${userId} or email: ${userEmail}. Skipping...`);
                continue;
            }

            const airtableRecordId = airtableRecord.id;
            console.log(`Found Airtable record ID: ${airtableRecordId} for user: ${userEmail}`);

            // Prepare data to update
            const emailRecord = emailEntry.record;
            const updateData = {
                'waka_last_synced_from_db': new Date().toISOString(),
                'waka_first_heartbeat': emailRecord.first_heartbeat || null,
                'waka_last_heartbeat': emailRecord.last_heartbeat || null,
                'waka_known_machine_count': Number(emailRecord.known_machine_count) === 0 ? null : Number(emailRecord.known_machine_count),
                'waka_known_machines': sanitizeAndSerialize(emailRecord.known_machines),
                'waka_30_day_active_machine_count': Number(emailRecord["30_day_active_machine_count"]) === 0 ? null : Number(emailRecord["30_day_active_machine_count"]),
                'waka_30_day_active_machines': sanitizeAndSerialize(emailRecord["30_day_active_machines"]),
                'waka_known_installation_count': Number(emailRecord.known_installation_count) === 0 ? null : Number(emailRecord.known_installation_count),
                'waka_known_installations': sanitizeAndSerialize(emailRecord.known_installations),
                'waka_30_day_active_installation_count': Number(emailRecord["30_day_active_installation_count"]) === 0 ? null : Number(emailRecord["30_day_active_installation_count"]),
                'waka_30_day_active_installations': sanitizeAndSerialize(emailRecord["30_day_active_installations"]),
                'waka_total_hours_logged': Number(emailRecord.total_hours_logged) === 0 ? null : Number(emailRecord.total_hours_logged),
                'waka_30_day_hours_logged': Number(emailRecord["30_day_hours_logged"]) === 0 ? null : Number(emailRecord["30_day_hours_logged"]),
            };

            console.log(`Preparing to update Airtable record for user: ${userEmail} with data:`, updateData);

            // Check if the record ID is already in the updateBatch
            if (updateBatch.has(airtableRecordId)) {
                // Compare the last_heartbeat to decide which one to keep
                const existingUpdate = updateBatch.get(airtableRecordId);
                const existingLastHeartbeat = existingUpdate.fields['waka_last_heartbeat'];
                if (emailRecord.last_heartbeat > existingLastHeartbeat) {
                    // Replace the existing entry with the new one
                    console.log(`Replacing existing update for record ID ${airtableRecordId} with more recent data.`);
                    updateBatch.set(airtableRecordId, {
                        id: airtableRecordId,
                        fields: updateData,
                    });
                } else {
                    // Do not replace; keep the existing one
                    console.log(`Existing update for record ID ${airtableRecordId} has more recent data. Skipping.`);
                }
            } else {
                // Add the update to the batch
                updateBatch.set(airtableRecordId, {
                    id: airtableRecordId,
                    fields: updateData,
                });
            }

            // If the updateBatch Map size reaches 10, send the batch update
            if (updateBatch.size === 10) {
                console.log(`Updating Airtable with batch of ${updateBatch.size} records...`);
                try {
                    await base(airtableTableName).update(Array.from(updateBatch.values()));
                    console.log(`Batch update successful.`);
                } catch (error) {
                    console.error('Error during batch update:', error);
                    airtableErrorOccurred = true;
                }
                updateBatch.clear();
            }
        }
    } catch (error) {
        console.error('Error during user lookup and update:', error);
        airtableErrorOccurred = true;
    }
}

// Start the process
processRecords();
