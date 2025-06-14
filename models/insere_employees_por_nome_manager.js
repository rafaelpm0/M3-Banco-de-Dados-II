import cassandra from 'cassandra-driver';
import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { connectToDatabase, closeDatabaseConnection } from '../DBs/mySql/mySql.js';

const { types } = cassandra;

// Busca um lote de funcionários a partir de um emp_no mínimo
async function fetchBatch(limit, lastEmpNo) {
  const sequelize = connectToDatabase();
  try {
    const [results] = await sequelize.query(`
      SELECT 
        e.emp_no,
        e.first_name,
        e.last_name,
        e.birth_date,
        e.gender,
        e.hire_date,
        mgr.first_name AS manager_first_name,
        mgr.last_name AS manager_last_name
      FROM employees e
      LEFT JOIN dept_emp de ON e.emp_no = de.emp_no
      LEFT JOIN departments d ON de.dept_no = d.dept_no
      LEFT JOIN dept_manager dm ON de.dept_no = dm.dept_no
        AND de.from_date <= dm.to_date AND de.to_date >= dm.from_date
      LEFT JOIN employees mgr ON dm.emp_no = mgr.emp_no
      WHERE e.emp_no > ${lastEmpNo}
      ORDER BY e.emp_no, dm.from_date
      LIMIT ${limit};
    `);
    return results;
  } finally {
    await closeDatabaseConnection(sequelize);
  }
}

// Função para garantir a extração da string da data YYYY-MM-DD, independente do tipo de dado
function getDateString(value) {
  if (!value) return null;
  if (typeof value === 'string') return value.slice(0, 10);
  if (value instanceof Date) return value.toISOString().slice(0, 10);
  return null;
}

// Converte string YYYY-MM-DD para objeto Date ajustado (sem deslocamento de timezone)
function toLocalDateFromString(dateStr) {
  if (!dateStr) return null;
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day);
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

      // Descubra o maior emp_no do lote
      const maxEmpNo = Math.max(...data.map(row => row.emp_no));

      // Separe as linhas do lote que têm o maxEmpNo
      const linhasMaxEmpNo = data.filter(row => row.emp_no === maxEmpNo);

      // Se o lote está "cortando" um emp_no, precisamos garantir que todas as linhas desse emp_no sejam processadas no próximo lote
      // Então, só atualize o lastEmpNo para maxEmpNo se TODAS as linhas desse emp_no já foram processadas
      // Para isso, só avance o lastEmpNo se o lote não está "cheio" (data.length < batchSize)
      // Ou, mais seguro: processe normalmente, mas no próximo lote, busque emp_no > lastEmpNo

      // Só insere se tiver nome de gerente
      const queries = data
        .filter(row => row.manager_first_name && row.manager_last_name)
        .map(row => ({
          query: `INSERT INTO employees_by_manager 
            (manager_name, emp_no, first_name, last_name, birth_date, gender, hire_date)
            VALUES (?, ?, ?, ?, ?, ?, ?)`,
          params: [
            `${row.manager_first_name} ${row.manager_last_name}`,
            row.emp_no,
            row.first_name,
            row.last_name,
            toLocalDateFromString(getDateString(row.birth_date)),
            row.gender,
            toLocalDateFromString(getDateString(row.hire_date))
          ]
        }));

      if (queries.length > 0) {
        await cassandraClient.batch(queries, { prepare: true });
        totalImportados += queries.length;
        console.log(`Lote importado: ${queries.length} funcionários (emp_no atual: ${lastEmpNo})`);
      }

      // Atualiza o lastEmpNo para garantir que não pule registros do mesmo emp_no
      // Se o lote está cheio e há mais de um emp_no igual ao último, continue no próximo lote
      if (data.length < batchSize) {
        // Último lote, pode avançar normalmente
        lastEmpNo = maxEmpNo;
      } else {
        // Pode haver mais registros com o mesmo emp_no no próximo lote
        lastEmpNo = maxEmpNo - 1;
      }
    }

    console.log(`Importação concluída: ${totalImportados} funcionários inseridos/atualizados.`);
  } catch (error) {
    console.error('Erro ao importar para o Cassandra:', error);
    throw error;
  } finally {
    if (cassandraClient) await closeCassandraConnection(cassandraClient);
  }
}

// Teste de execução
importCassandra(1000).catch(err => console.error('Erro na execução da importação:', err));
