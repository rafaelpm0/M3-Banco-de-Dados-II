import cassandra from 'cassandra-driver';
import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { connectToDatabase, closeDatabaseConnection } from '../DBs/mySql/mySql.js';

const { types } = cassandra;

// Função para buscar um lote de funcionários usando LIMIT e OFFSET
async function fetchBatch(limit, offset) {
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
        s.salary,
        s.from_date AS salary_from_date,
        s.to_date AS salary_to_date,
        t.title,
        t.from_date AS title_from_date,
        t.to_date AS title_to_date,
        d.dept_name,
        de.from_date AS dept_emp_from_date,
        de.to_date AS dept_emp_to_date,
        de.dept_no AS emp_dept_no,
        dm.emp_no AS manager_emp_no,
        dm.dept_no AS manager_dept_no,
        d.dept_name AS manager_dept_name,
        dm.from_date AS manager_from_date,
        dm.to_date AS manager_to_date,
        mgr.first_name AS manager_first_name,
        mgr.last_name AS manager_last_name
      FROM 
        employees e
      LEFT JOIN 
        salaries s ON e.emp_no = s.emp_no
      LEFT JOIN 
        titles t ON e.emp_no = t.emp_no
      LEFT JOIN 
        dept_emp de ON e.emp_no = de.emp_no
      LEFT JOIN 
        departments d ON de.dept_no = d.dept_no
      LEFT JOIN dept_manager dm ON de.dept_no = dm.dept_no
        AND de.from_date <= dm.to_date AND de.to_date >= dm.from_date
      LEFT JOIN employees mgr ON dm.emp_no = mgr.emp_no
      LIMIT ${limit} OFFSET ${offset};
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
  // Criar objeto Date com ano, mês (0-based) e dia
  const [year, month, day] = dateStr.split('-').map(Number);
  return new Date(year, month - 1, day);
}

async function importCassandra(batchSize = 100, cassandraLimit = 10, totalLimit = null) {
  let offset = 0;
  let totalImportados = 0;
  let cassandraClient;

  try {
    cassandraClient = connectToCassandra();
    await cassandraClient.connect();

    while (true) {
      if (totalLimit !== null && totalImportados >= totalLimit) break;

      const data = await fetchBatch(batchSize, offset);
      if (data.length === 0) break;

      const grouped = {};
      for (const row of data) {
        const emp_no = row.emp_no;
        if (!grouped[emp_no]) {
          grouped[emp_no] = {
            emp_no: emp_no,
            first_name: row.first_name,
            last_name: row.last_name,
            birth_date: toLocalDateFromString(getDateString(row.birth_date)),
            gender: row.gender,
            hire_date: toLocalDateFromString(getDateString(row.hire_date)),
            salaries: [],
            titles: [],
            departments: []
          };
        }

        // Salários
        if (
          row.salary != null &&
          row.salary_from_date &&
          row.salary_to_date
        ) {
          const fromDate = toLocalDateFromString(getDateString(row.salary_from_date));
          const toDate = toLocalDateFromString(getDateString(row.salary_to_date));
          if (fromDate && toDate) {
            const tuple = new types.Tuple(parseInt(row.salary), fromDate, toDate);
            const exists = grouped[emp_no].salaries.some(
              s => s.elements[0] === tuple.elements[0] &&
                   s.elements[1].getTime() === tuple.elements[1].getTime() &&
                   s.elements[2].getTime() === tuple.elements[2].getTime()
            );
            if (!exists) {
              grouped[emp_no].salaries.push(tuple);
            }
          }
        }

        // Títulos
        if (
          row.title &&
          row.title_from_date &&
          row.title_to_date
        ) {
          const fromDate = toLocalDateFromString(getDateString(row.title_from_date));
          const toDate = toLocalDateFromString(getDateString(row.title_to_date));
          if (fromDate && toDate) {
            const tuple = new types.Tuple(row.title, fromDate, toDate);
            const exists = grouped[emp_no].titles.some(
              t => t.elements[0] === tuple.elements[0] &&
                   t.elements[1].getTime() === tuple.elements[1].getTime() &&
                   t.elements[2].getTime() === tuple.elements[2].getTime()
            );
            if (!exists) {
              grouped[emp_no].titles.push(tuple);
            }
          }
        }

        // Departamentos
        if (
          row.dept_name &&
          row.dept_emp_from_date &&
          row.dept_emp_to_date
        ) {
          const fromDate = toLocalDateFromString(getDateString(row.dept_emp_from_date));
          const toDate = toLocalDateFromString(getDateString(row.dept_emp_to_date));
          if (fromDate && toDate) {
            const tuple = new types.Tuple(
              row.dept_name,
              row.emp_dept_no, // já está string, conforme solicitado
              fromDate,
              toDate,
              row.manager_first_name && row.manager_last_name
                ? `${row.manager_first_name} ${row.manager_last_name}`
                : null,
              row.manager_emp_no ? parseInt(row.manager_emp_no, 10) : null
            );
            const exists = grouped[emp_no].departments.some(
              d =>
                d.elements[0] === tuple.elements[0] &&
                d.elements[2].getTime() === tuple.elements[2].getTime() &&
                d.elements[3].getTime() === tuple.elements[3].getTime() &&
                d.elements[5] === tuple.elements[5]
            );
            if (!exists) grouped[emp_no].departments.push(tuple);
          }
        }
      }

      let employees = Object.values(grouped).slice(0, cassandraLimit);

      if (totalLimit !== null && totalImportados + employees.length > totalLimit) {
        employees = employees.slice(0, totalLimit - totalImportados);
      }

      // Log de exemplo
      //console.log("Exemplo de funcionário preparado para Cassandra:");
      //console.dir(employees[0], { depth: null });

      const queries = employees.map(emp => ({
        query: `INSERT INTO employees.pessoas 
          (emp_no, first_name, last_name, birth_date, gender, hire_date, salaries, titles, departments)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
        params: [
          emp.emp_no,
          emp.first_name,
          emp.last_name,
          emp.birth_date,
          emp.gender,
          emp.hire_date,
          emp.salaries,
          emp.titles,
          emp.departments
        ]
      }));

      if (queries.length > 0) {
        await cassandraClient.batch(queries, { prepare: true });
        totalImportados += queries.length;
        console.log(`Lote importado: ${queries.length} funcionários (offset atual: ${offset})`);
      }

      offset += batchSize;
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
importCassandra(1000, 500).catch(err => console.error('Erro na execução da importação:', err));
