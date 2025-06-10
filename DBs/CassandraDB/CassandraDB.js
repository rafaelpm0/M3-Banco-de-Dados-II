let credentials = {
  // Substitua pelos dados de conexão
};

import { Client } from "cassandra-driver";

/**
 * Função para criar uma conexão com o banco de dados Cassandra.
 * @returns {Client} - Instância do Cassandra Client conectada ao banco de dados.
 */
function connectToCassandra() {
  const client = new Client({
    cloud: {
      secureConnectBundle: 'path/to/secure-connect-database_name.zip', // Substitua pelo caminho do seu bundle de conexão segura
    },
    credentials: {
      username: credentials.clientId,
      password: credentials.secret,
    },
  });
  return client;
}

/**
 * Função para encerrar a conexão com o banco de dados Cassandra.
 * @param {Client} client - Instância do Cassandra Client conectada ao banco de dados.
 */
async function closeCassandraConnection(client) {
  try {
    await client.shutdown();
  } catch (error) {
    console.error('Erro ao encerrar a conexão com o Cassandra:', error.message);
  }
}

export { connectToCassandra, closeCassandraConnection };
