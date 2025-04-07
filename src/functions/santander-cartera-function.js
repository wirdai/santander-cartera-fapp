const { app } = require("@azure/functions");
const { getRowsFromRequestCSV } = require("../utils/csv");
const {
  getClickhouseClient,
  getSantanderUsersFromClickhouse,
  insertRecordsIntoClickhouse,
} = require("../utils/clickhouse");

const VALUES = ["id_cargo", "descripcion_cargo", "cartera", "cartera_original", "codsuc_comercial"];

const handleRequest = async (request, _context) => {
  console.info("Starting process request");
  const rows = await getRowsFromRequestCSV(request);
  if (!rows) {
    console.error("Error, invalid CSV file");
    return {
      status: 400,
      body: "Invalid CSV file",
    };
  }
  console.info("Got rows from CSV");
  // Parse rows and create a record by email
  // email: { user_id, email, id_cargo, cartera, cartera_original, codsuc_comercial }
  // user_id will bet get from Clickhouse in the next step
  const recordByEmail = {};
  for (const row of rows) {
    const [_type, email, key, value, _date] = row;
    // Email must not be undefined and key must be one of the following
    if (email && VALUES.includes(key)) {
      const emailLowerCase = email.toLowerCase();
      if (!recordByEmail[emailLowerCase])
        recordByEmail[emailLowerCase] = { email: emailLowerCase };
      recordByEmail[emailLowerCase][key] = value;
    }
  }
  console.info("Parsed rows into records");
  const clickhouseClient = getClickhouseClient();

  // Set user_id for each record and remove records without user_id
  // because they were not found in Clickhouse
  const users = await getSantanderUsersFromClickhouse(clickhouseClient);
  console.info("Got users from Clickhouse");
  for (const user of users) {
    const { id, email } = user;
    const record = recordByEmail[email.toLowerCase()];
    if (record) record["user_id"] = id;
  }
  const records = Object.values(recordByEmail).filter((r) => r["user_id"]);
  console.info("Set user_id for each record");

  // Insert Santander Cartera records into Clickhouse
  await insertRecordsIntoClickhouse(clickhouseClient, records);
  console.info("Successfully inserted records into Clickhouse!");

  return {
    status: 200,
    jsonBody: records,
  };
};

app.http("cartera", {
  methods: ["GET", "POST"],
  authLevel: "anonymous",
  handler: handleRequest,
});
