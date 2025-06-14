import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { types } from 'cassandra-driver';

export async function getEmployeesByManager(managerName) {
  const client = connectToCassandra();
  try {
    await client.connect();
    const query = `
      SELECT emp_no, first_name, last_name, birth_date, gender, hire_date
      FROM employees_by_manager
      WHERE manager_name = ?
    `;
    const result = await client.execute(query, [managerName], { prepare: true, fetchSize: 100000 });
    return result.rows;
  } catch (error) {
    console.error('Erro ao buscar funcionários pelo nome do gerente:', error);
    throw error;
  } finally {
    await closeCassandraConnection(client);
  }
}

// Teste de execução direto via ES module
const nomeGerente = 'Oscar Ghazalie'; // Troque pelo nome desejado
getEmployeesByManager(nomeGerente)
  .then(employees => {
    // Converte datas LocalDate para string legível
    employees.forEach(emp => {
      if (emp.birth_date instanceof types.LocalDate) emp.birth_date = emp.birth_date.toString();
      if (emp.hire_date instanceof types.LocalDate) emp.hire_date = emp.hire_date.toString();
    });
    console.table(employees);
    console.log(`O gerente: ${nomeGerente}, tem ${employees.length} funcionário(s) relacionados a ele.`);
  })
  .catch(err => console.error('Erro:', err));