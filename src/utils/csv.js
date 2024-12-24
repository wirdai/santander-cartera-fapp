const XLSX = require("xlsx");

const getRowsFromRequestCSV = async (request) => {
  const fileBuffer = await request.arrayBuffer();
  const workbook = XLSX.read(fileBuffer);
  const sheetName = workbook.SheetNames[0];
  const worksheet = workbook.Sheets[sheetName];
  const jsonData = XLSX.utils.sheet_to_json(worksheet, {
    header: 1,
    raw: false,
  });

  const headerRowNumber = jsonData.findIndex(
    (row) =>
      row.length === 5 &&
      row[0] === "tipo" &&
      row[1] === "llave" &&
      row[2] === "caracteristica" &&
      row[3] === "valor" &&
      row[4] === "data_date_part"
  );

  if (headerRowNumber === -1) return null;

  const rows = jsonData.slice(headerRowNumber + 1);

  return rows;
};

module.exports = {
  getRowsFromRequestCSV,
};
