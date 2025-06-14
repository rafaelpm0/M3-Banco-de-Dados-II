import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';

async function getEmpNos(managerName) {
  const client = connectToCassandra();
  try {
    await client.connect();
    const result = await client.execute(
      `SELECT emp_no FROM employees_by_manager WHERE manager_name = ?`,
      [managerName],
      { prepare: true, fetchSize: 100000 }
    );
    const empNos = result.rows.map(row => row.emp_no);
    console.log(empNos.join(','));
  } finally {
    await closeCassandraConnection(client);
  }
}

getEmpNos('Shem Kieras');