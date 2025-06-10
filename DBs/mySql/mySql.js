import { Sequelize } from 'sequelize';

/**
 * Função para criar uma conexão com o banco de dados MySQL usando Sequelize.
 * @returns {Sequelize} - Instância do Sequelize conectada ao banco de dados.
 */
export function connectToDatabase() {
  const sequelize = new Sequelize('employees', 'root', 'root', {
    host: 'localhost',
    dialect: 'mysql',
    logging: false,
  });

  sequelize
    .authenticate()

  return sequelize;
}

/**
 * Função para encerrar a conexão com o banco de dados MySQL.
 * @param {Sequelize} sequelize - Instância do Sequelize conectada ao banco de dados.
 */
export async function closeDatabaseConnection(sequelize) {
  try {
    await sequelize.close();
  } catch (error) {
    console.error('Erro ao encerrar a conexão com o banco de dados:', error.message);
  }
}


export default { connectToDatabase, closeDatabaseConnection };

/**
 * Função de teste local para verificar a conexão com o banco de dados.
 */
async function testDatabaseConnection() {
  const sequelize = connectToDatabase(); // Conecta ao banco de dados

  try {
    await sequelize.authenticate(); // Testa a autenticação
    console.log('Conexão com o banco de dados bem-sucedida.');

    // Aqui você pode adicionar testes adicionais, como listar tabelas ou executar consultas
  } catch (error) {
    console.error('Erro ao conectar ao banco de dados:', error.message);
  } finally {
    // Encerra a conexão
    await closeDatabaseConnection(sequelize);
  }
}

// Executa o teste local
testDatabaseConnection();
