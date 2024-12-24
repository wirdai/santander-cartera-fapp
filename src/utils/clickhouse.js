const { createClient } = require("@clickhouse/client");

const getClickhouseClient = () => {
  const client = createClient({
    url: process.env.CLICKHOUSE_URL,
    username: process.env.CLICKHOUSE_USER,
    password: process.env.CLICKHOUSE_PASSWORD,
  });

  return client;
};

const getSantanderUsersFromClickhouse = async (clickhouseClient) => {
  const query = `
      SELECT email, id
      FROM wmapp.user
      WHERE customer_id = 211 AND deleted = 0
      ORDER BY email
    `;
  const result = await clickhouseClient.query({
    query,
    format: "JSONEachRow",
  });
  const users = await result.json();
  return users;
};

const insertRecordsIntoClickhouse = async (clickhouseClient, records) => {
  await clickhouseClient.insert({
    table: "wirdflow.santander_cartera",
    values: records,
    format: "JSONEachRow",
  });
};

module.exports = {
  getClickhouseClient,
  getSantanderUsersFromClickhouse,
  insertRecordsIntoClickhouse,
};
