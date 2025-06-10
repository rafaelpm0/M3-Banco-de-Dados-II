import { connectToCassandra, closeCassandraConnection } from '../DBs/CassandraDB/CassandraDB.js';
import { connectToDatabase, closeDatabaseConnection } from '../DBs/mySql/mySql.js';

async function main() {
  // Conexão MySQL
  const sequelize = connectToDatabase();
  try {
    await sequelize.authenticate();
    console.log('Conexão com MySQL bem-sucedida.');
  } catch (error) {
    console.error('Erro ao conectar ao MySQL:', error.message);
  }

  // Conexão Cassandra
  const cassandraClient = connectToCassandra();
  try {
    await cassandraClient.connect();
    console.log("Connected to Astra DB");

    // Cria a tabela se não existir
    await cassandraClient.execute(`
      CREATE TABLE IF NOT EXISTS employees.pessoas (
        id UUID PRIMARY KEY,
        nome TEXT
      )
    `);

    // Insere um dado
    const { v4: uuidv4 } = await import('uuid');
    const id = uuidv4();
    const nome = 'João da Silva';
    await cassandraClient.execute(
      'INSERT INTO employees.pessoas (id, nome) VALUES (?, ?)',
      [id, nome],
      { prepare: true }
    );
    console.log('Dado inserido.');

    // Mostra o dado inserido
    let rs = await cassandraClient.execute(
      'SELECT * FROM employees.pessoas WHERE id = ?',
      [id],
      { prepare: true }
    );
    console.log('Dado encontrado:', rs.rows);

    // Exclui o dado
    await cassandraClient.execute(
      'DELETE FROM employees.pessoas WHERE id = ?',
      [id],
      { prepare: true }
    );
    console.log('Dado excluído.');

    // Mostra novamente (deve estar vazio)
    rs = await cassandraClient.execute(
      'SELECT * FROM employees.pessoas WHERE id = ?',
      [id],
      { prepare: true }
    );
    console.log('Após exclusão:', rs.rows);

  } catch (error) {
    console.error("Erro ao operar no Cassandra:", error);
  } finally {
    await closeCassandraConnection(cassandraClient);
    await closeDatabaseConnection(sequelize);
    console.log("Connections closed");
  }
}

main();