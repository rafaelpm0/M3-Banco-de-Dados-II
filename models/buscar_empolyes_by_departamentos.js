import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { connectToDatabase, closeDatabaseConnection } from '../DBs/mySql/mySql.js';

// Busca um lote de funcionários a partir de um intervalo de datas e nome do departamento
async function fetchBatch(batchSize = 1000, lastEmpNo = 0) {
  const sequelize = connectToDatabase();
  try {
    const [results] = await sequelize.query(`
      SELECT 
        a.emp_no, 
        a.birth_date, 
        a.first_name, 
        a.last_name, 
        a.gender, 
        a.hire_date, 
        b.dept_no, 
        c.dept_name, 
        b.from_date, 
        b.to_date
      FROM employees a
      LEFT JOIN dept_emp b
        ON a.emp_no = b.emp_no
      LEFT JOIN departments c
        ON b.dept_no = c.dept_no
      WHERE b.from_date >= '2000-01-01'
        AND b.to_date <= '2020-12-31'
        AND a.emp_no > ${lastEmpNo}
      ORDER BY a.emp_no, b.from_date
      LIMIT ${batchSize};
    `);
    return results;
  } finally {
    await closeDatabaseConnection(sequelize);
  }
}

async function importCassandra(batchSize = 1000, totalLimit = null) {
  let lastEmpNo = 0;
  let totalImportados = 0;
  let cassandraClient;

  try {
    cassandraClient = connectToCassandra();
    await cassandraClient.connect();

    while (true) {
      if (totalLimit !== null && totalImportados >= totalLimit) break;

      const data = await fetchBatch(batchSize, lastEmpNo);
      if (data.length === 0) break;

      const maxEmpNo = Math.max(...data.map(row => row.emp_no));

      // Preparar INSERTs
      const queries = data.map(row => ({
        query: `
          INSERT INTO employees_by_dept (
            emp_no, birth_date, first_name, last_name, gender,
            hire_date, dept_no, dept_name, from_date, to_date
          ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        `,
        params: [
          row.emp_no,
          row.birth_date,
          row.first_name,
          row.last_name,
          row.gender,
          row.hire_date,
          row.dept_no,
          row.dept_name,
          row.from_date,
          row.to_date
        ]
      }));

      if (queries.length > 0) {
        try {
          await cassandraClient.batch(queries, { prepare: true });
        } catch (error) {
          console.error("Erro ao inserir lote:", error);
        }

        totalImportados += queries.length;
        console.log(`Lote importado: ${queries.length} funcionários (emp_no atual: ${lastEmpNo})`);
      }

      // Atualiza lastEmpNo
      lastEmpNo = data.length < batchSize ? maxEmpNo : maxEmpNo - 1;
    }

    console.log(`Importação concluída: ${totalImportados} funcionários inseridos/atualizados.`);
  } catch (error) {
    console.error('Erro ao importar para o Cassandra:', error);
    throw error;
  } finally {
    if (cassandraClient) await closeCassandraConnection(cassandraClient);
  }
}

export { fetchBatch, importCassandra };

// Função de teste
async function teste() {
  await importCassandra(1000, null);
}

teste();
