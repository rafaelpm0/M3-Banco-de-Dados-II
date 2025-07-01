import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { connectToDatabase, closeDatabaseConnection } from '../DBs/mySql/mySql.js';

// Busca médias salariais por departamento no MySQL
async function fetchAverageSalaries() {
  const sequelize = connectToDatabase();
  try {
    const [results] = await sequelize.query(`
      SELECT 
        a.dept_no, 
        c.dept_name, 
        AVG(b.salary) AS avg_salaries
      FROM dept_emp a
      LEFT JOIN salaries b ON a.emp_no = b.emp_no
      LEFT JOIN departments c ON a.dept_no = c.dept_no
      GROUP BY a.dept_no;
    `);
    return results;
  } finally {
    await closeDatabaseConnection(sequelize);
  }
}

// Importa os dados para o Cassandra
async function importAvgSalariesToCassandra() {
  let cassandraClient;

  try {
    cassandraClient = connectToCassandra();
    await cassandraClient.connect();

    const data = await fetchAverageSalaries();

    const queries = data.map(row => ({
      query: `
        INSERT INTO avg_salary_by_dept (dept_no, dept_name, avg_salaries)
        VALUES (?, ?, ?)
      `,
      params: [row.dept_no, row.dept_name, parseFloat(row.avg_salaries)]
    }));

    if (queries.length > 0) {
      await cassandraClient.batch(queries, { prepare: true });
      console.log(`Inseridos ${queries.length} departamentos com média salarial.`);
    } else {
      console.log('Nenhum dado retornado para inserção.');
    }
  } catch (error) {
    console.error('Erro na importação:', error);
  } finally {
    if (cassandraClient) await closeCassandraConnection(cassandraClient);
  }
}


// Teste de execução
importAvgSalariesToCassandra();
